;; This namespace provides functions for unpacking HDF files storing
;; NASA MODIS data. The functions here attempt to apply generally to
;; all MODIS datasets, though further work must be done to verify this
;; fact.
;;
;; The overall goal here is to process a MODIS HDF4 archive into
;; a forma.schema/chunk-value.
;;
;; If every dataset can be coerced into this form, it becomes trivial
;; to match tuples up and combine them in various ways.

(ns forma.source.hdf
  (:use cascalog.api
        [cascalog.util :only (uuid)]
        [forma.reproject :only (spatial-res temporal-res tilestring->hv)])
  (:require [clojure.set :as set]
            [forma.utils :as u]
            [forma.thrift :as thrift]
            [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]
            [cascalog.io :as io]
            [clojure.java.io :as java.io])
  (:import [org.gdal.gdal gdal Dataset Band]))

;; ## MODIS Introduction
;;
;; See the [MODIS wiki page](http://goo.gl/byMfF) for detailed
;; information on the program. For specific information on the MOD13
;; products used by REDD as of 02/2011, see the excellent [MOD13
;; User's Guide](http://goo.gl/sMEJG), produced by the University of
;; Arizona.
;;
;; ### Constants
;;
;; Every MODIS file, packaged using the HDF4 compression format,
;; packages a number of different datasets; 9, in the case of the
;; MOD13A3 product. FORMA, in its first iteration, uses one of these
;; layers, the [Normalized Difference Vegetation
;; Index](http://goo.gl/kjojh), or NDVI. NASA doesn't provide a short
;; identifier for each data layer within its products, as it does with
;; the products themselves; as a solution, we parse the long-form
;; subdataset file paths for substrings that remain constant for each
;; data layer across spatial and temporal resolutions.

(def modis-subsets
  "Map between MODIS dataset identifiers (arbitrarily chosen by the
  REDD team) and corresponding unique substrings, culled from values
  in the \"NAMES\" entries of the SUBDATASETS hashtable of MOD13A3 HDF
  file. No simple, descriptive tag exists within the metadata, as of
  2/05/2011, so this ends up being the best solution."
  {:evi "EVI"
   :qual "VI Quality"
   :red "red reflectance"
   :mir "MIR reflectance"
   :nir "NIR reflectance"
   :azi "azimuth"
   :blue "blue reflectance"
   :reli "pixel"
   :viewz "view zenith"
   :ndvi "NDVI"
   :sunz "sun zenith"})

;; ### HDF4 Decompression
;;
;; Each HDF4 archive contains a good deal of metadata about the
;; packaged MODIS product. The metadata is fairly disorganized, but
;; provides us with everything we need for proper classification of
;; the data we need.

(defn with-gdal-open* [f path]
  (let [gdal (do (gdal/AllRegister)
                 (gdal/Open path))]
    (try (f gdal)
         (finally (.delete gdal)))))

(defmacro with-gdal-open
  "Accepts a vector containing a symbol and a path to a file capable
  of being opened with `gdal/Open`, and binds the opened dataset to
  the supplied symbol."
  [[sym path] & body]
  `(with-gdal-open* (fn [~sym] ~@body) ~path))

(defn metadata
  "Returns the metadata map for the supplied MODIS Dataset."
  ([modis]
     (metadata modis ""))
  ([^Dataset modis key]
     (into {} (.GetMetadata_Dict modis key))))

;; The "SUBDATASETS" metadata dictionary contains 
;;
;; The paths to these are contained within the "SUBDATASETS" metadata
;; dictionary of the wrapping archive. This dictionary contains 18
;; groups of K-V pairs, or 2 per dataset:
;;
;;      Key: SUBDATASET_2_NAME
;;      Val: <hdf stuff>:"/path/to/modis.hdf":
;;           MOD_Grid_monthly_1km_VI:
;;           1 km monthly EVI
;;      Key: SUBDATASET_4_DESC
;;      Val: [1200x1200] 1 km monthly EVI
;;           MOD_Grid_monthly_1km_VI
;;           (16-bit integer)
;;
;; the `DESC` key is redundant, so we filter for `_NAMES` only. The
;; string at the end of the `_NAMES` value allows us to identify the
;; specific subdataset, and filter it against `modis-subsets`.

(defn subdataset-names
  "Returns the NAME entries of the SUBDATASETS metadata map for the
  dataset at a given filepath."
  [hdf-path]
  (with-gdal-open [dataset (str hdf-path)]
    (for [[^String k v] (metadata dataset "SUBDATASETS")
          :when (.contains k "_NAME")]
      v)))

(defn subdataset-key
  "Takes a long-form path to a MODIS subdataset, and checks to see if
  any of the values of the `modis-subsets` map can be found as
  substrings. If we find one, we return the associated key, cast to a
  string -- 'ndvi', for example."
  [^String path]
  (u/find-first #(.contains path (% modis-subsets))
                (keys modis-subsets)))

(defn dataset-filter
  "Generates a predicate function that checks a name from the
   SUBDATASETS metadata dictionary against the supplied collection of
   acceptable dataset keys."
  [good-keys]
  (fn [^String name]
    {:pre [(set/subset? good-keys modis-subsets)]}
    (->> good-keys
         (map modis-subsets)
         (some #(.contains name %)))))

(defn make-subdataset
  "Accepts a filepath from the SUBDATASETS dictionary of a MODIS Dataset,
 and returns a 2-tuple consisting of the modis-subsets key (\"ndvi\")
  and a gdal.Dataset object representing the unpacked MODIS data."
  [path]
  [(name (subdataset-key path))
   (gdal/Open path)])

;; This is the first real "director" function; cascalog calls feeds
;; `BytesWritable` objects into `unpack-modis` and receives individual
;; datasets back.

;; TODO: Update documentation with return value.

(defmapcatop [unpack-modis [to-keep]]
  "Stateful approach to unpacking HDF files. Registers all gdal
formats, Creates a temp directory, then saves the byte array to
disk. This byte array is processed with gdal. On teardown, the temp
directory is destroyed. Function returns the decompressed MODIS file
as a 1-tuple."
  {:stateful true}
  ([] (io/temp-dir "hdf"))
  ([tdir stream]
     (let [bytes    (io/get-bytes stream)
           temp-hdf (java.io/file tdir (uuid))]
       (java.io/copy bytes temp-hdf)
       (->> (subdataset-names temp-hdf)
            (filter (dataset-filter to-keep))
            (map make-subdataset))))
  ([tdir] (io/delete-file-recursively tdir)))

;; ### Raster Chunking
;;
;; At this point in the process, we're operating on a single dataset,
;; and breaking it into chunks of pixels. We do this to allow our
;;functions to scale directly with pixel count, rather than with
;;number of datasets processed.

(defmapcatop [raster-chunks [chunk-size]]  
  "Unpacks the data inside of a MODIS band and partitions it into
  chunks sized according to the supplied value. Specifically, returns
  a lazy sequence of 2-tuples of the form `[chunk-index, vector]`."
  [^Dataset data]
  (let [^Band band (.GetRasterBand data 1)
        width  (.GetXSize band)
        height (.GetYSize band)
        ret (int-array (* width height))]
    (.ReadRaster band 0 0 width height ret)
    (map-indexed (fn [idx xs]
                   [idx (vec xs)])
                 (partition chunk-size ret))))

;; ### Metadata Parsing
;;
;; After unpacking the dataset from the HDF archive, we still need to
;; harvest various metadata values to convert into fields in our ideal
;; tuple, described above. The following functions provide
;; MODIS-specific facilities for making this happen.

(defmapop [meta-values [meta-keys]]
  "Returns metadata values for a given unpacked MODIS Dataset,
  corresponding to the supplied seq of keys."
  [dataset]
  (map (metadata dataset) meta-keys))

;; The MODLAND Tile ID is an 8 digit integer, such as 51018009, that
;; is used to specify a gridded product's projection, horizontal and
;; vertical tile number.
;;
;; The first digit is used to identify the projection. REDD deals with
;; MODIS products from collection 5, which uses a sinusoial
;; projection.
;;
;; (We place a pre-condition on split-id to catch any errors resulting
;; from a dataset that uses a different projection. This may be
;; updated in future versions, if we decide to incorporate older MODIS
;; products.)

(defn tileid->res
  "Returns a string representation of the resolution (in meters) of
  the tile data referenced by the supplied TileID.The second character
  of a MODIS TileID acts as a key to retrieve this data."
  [tileid]
  (let [s (subs tileid 1 2)]
    (case s
      "1" "1000"
      "2" "500"
      "4" "250")))

(defn split-id
  "Returns a sequence containing the modis h and v coordinates, where
  h and v refer to the MODIS tile referenced by the supplied
  TileID. The precondition makes sure that the processed product uses
  a sinusoial projection."
  [tileid]
  {:pre [(= (first tileid) \5)]}
  (tilestring->hv (subs tileid 2)))

;; ### The Chunker!
;;
;; The chunker can't be refactored, as `org.gdal.gdal.Dataset` doesn't
;; implement `java.io.Serializable`. This forces us to completely
;; tease apart the HDF files supplied by the source before we can
;; return tuples from the subquery. Each MODIS file, even down to 250m
;; data, is smaller than 64MB, so sending each file to a different
;; mapper won't present any problems.

(defn modis-chunks
  "Takes a cascading source, and returns a number of tuples that fully
  describe chunks of MODIS data for the supplied datasets. Chunks are
  represented as seqs of floats. Be sure to convert chunks to vector
  before running any sort of data analysis, as seqs require linear
  time for lookups."
  [datasets chunk-size source]
  (let [keys ["SHORTNAME" "TileID" "RANGEBEGINNINGDATE"]
        chunkifier (p/chunkify chunk-size)]
    (<- [?datachunk]
        (source _ ?hdf)
        (unpack-modis [datasets] ?hdf :> ?dataset ?freetile)
        (raster-chunks [chunk-size] ?freetile :> ?chunkid ?chunk)
        (meta-values [keys] ?freetile :> ?productname ?tileid ?date)
        (split-id ?tileid :> ?mod-h ?mod-v)
        ((c/juxt #'spatial-res #'temporal-res) ?productname :> ?s-res ?t-res)
        (thrift/pack ?chunk :> ?array)
        (chunkifier ?dataset ?date ?s-res ?t-res ?mod-h ?mod-v ?chunkid ?array :> ?datachunk))))
