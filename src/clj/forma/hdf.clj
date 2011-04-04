;; This namespace provides functions for unpacking HDF files storing
;; NASA MODIS data. The functions here attempt to apply generally to
;; all MODIS datasets, though further work must be done to verify this
;; fact.
;;
;; The overall goal here is to process a MODIS HDF4 archive into
;; tuples of the form
;;
;;     [?dataset ?spatial-res ?temporal-res
;;      ?tilestring ?date ?chunkid ?chunk-pix]
;;
;; If every dataset can be coerced into this form, it becomes trivial
;; to match tuples up and combine them in various ways.

(ns forma.hdf
  (:use cascalog.api
        forma.hadoop
        [forma.modis :only (temporal-res)]
        [cascalog.io :only (temp-dir)]
        [clojure.contrib.seq-utils :only (find-first indexed)])
  (:require [clojure.contrib.string :as s]
            [clojure.contrib.io :as io])
  (:import [java.util Hashtable]
           [java.io File]
           [org.gdal.gdal gdal Dataset Band]
           [org.gdal.gdalconst gdalconstConstants]))

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

(def
  ^{:doc "Map between MODIS dataset identifiers (arbitrarily chosen by
  the REDD team) and corresponding unique substrings, culled from
  values in the \"NAMES\" entries of the SUBDATASETS hashtable of
  MOD13A3 HDF file. No simple, descriptive tag exists within the
  metadata, as of 2/05/2011, so this ends up being the best
  solution."}
  modis-subsets {:evi "monthly EVI"
   :qual "VI Quality"
   :red "red reflectance"
   :nir "NIR reflectance"
   :azi "azimuth"
   :blue "blue reflectance"
   :reli "monthly pixel"
   :viewz "view zenith"
   :ndvi "NDVI"
   :sunz "sun zenith"})

;; ### HDF4 Decompression
;;
;; Each HDF4 archive contains a good deal of metadata about the
;; packaged MODIS product. The metadata is fairly disorganized, but
;; provides us with everything we need for proper classification of
;; the data we need.

(defn metadata
  "Returns the metadata hashtable for the supplied MODIS Dataset."
  ([^Dataset modis key]
     (.GetMetadata_Dict modis key))
  ([modis]
     (metadata modis "")))

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
  "Returns the NAME entries of the SUBDATASETS metadata Hashtable for
  the dataset at a given filepath."
  [hdf-file]
  (let [path (str hdf-file)
        dataset (gdal/Open path)]
    (try
      (vals (filter #(s/substring? "_NAME" (key %))
                    (metadata dataset "SUBDATASETS")))
      (finally (.delete dataset)))))

(defn subdataset-key
  "Takes a long-form path to a MODIS subdataset, and checks to see if
  any of the values of the modis-subsets map can be found as
  substrings. If we find one, we return the associated key, cast to a
  string -- 'ndvi', for example."
  [path]
  (s/as-str (find-first #(s/substring? (% modis-subsets) path)
                        (keys modis-subsets))))

(defn dataset-filter
  "Generates a predicate function that checks a name from the
   SUBDATASETS metadata dictionary against the supplied collection of
   acceptable dataset keys."
  [good-keys]
  (let [substrings (map modis-subsets good-keys)]
    (fn [name]
      (some #(s/substring? % name) substrings))))

(defn make-subdataset
  "Accepts a filepath from the SUBDATASETS dictionary of a MODIS Dataset,
 and returns a 2-tuple consisting of the modis-subsets key (\"ndvi\")
  and a gdal.Dataset object representing the unpacked MODIS data."
  [path]
  (vector (subdataset-key path) (gdal/Open path)))

;; This is the first real "director" function; cascalog calls feeds
;; `BytesWritable` objects into `unpack-modis` and receives individual
;; datasets back.

(defmapcatop [unpack-modis [to-keep]] {:stateful true}
  ^{:doc "Stateful approach to unpacking HDF files. Registers all
gdal formats, Creates a temp directory, then saves the byte array to
disk. This byte array is processed with gdal. On teardown, the temp
directory is destroyed. Function returns the decompressed MODIS file
as a 1-tuple."}
  ([]
     (do
       (gdal/AllRegister)
       (temp-dir "hdf")))
  ([tdir stream]
     (let [bytes (get-bytes stream)
           temp-hdf (io/file tdir (hash-str stream))
           keep? (dataset-filter to-keep)]
       (do
         (io/copy bytes temp-hdf)
         (map make-subdataset
              (filter keep? (subdataset-names temp-hdf))))))
  ([tdir]
     (io/delete-file-recursively tdir)))

;; ### Raster Chunking
;;
;; At this point in the process, we're operating on a single dataset,
;; and breaking it into chunks of pixels. We do this to allow our
;;functions to scale directly with pixel count, rather than with
;;number of datasets processed.

(defmapcatop [raster-chunks [chunk-size]]  
  ^{:doc "Unpacks the data inside of a MODIS band and partitions it
  into chunks sized according to the supplied value. Specifically,
  returns a lazy sequence of 2-tuples of the form (chunk-index,
  int-array)."}
  [^Dataset data]
  (let [^Band band (.GetRasterBand data 1)
        width (.GetXSize band)
        height (.GetYSize band)
        ret (int-array (* width height))]
    (.ReadRaster band 0 0 width height ret)
    (indexed (partition chunk-size ret))))

;; ### Metadata Parsing
;;
;; After unpacking the dataset from the HDF archive, we still need to
;; harvest various metadata values to convert into fields in our ideal
;; tuple, described above. The following functions provide
;; MODIS-specific facilities for making this happen.

(defmapop [meta-values [meta-keys]]
  ^{:doc "Returns metadata values for a given unpacked MODIS Dataset,
  corresponding to the supplied seq of keys."}
  [modis]
  (let [^Hashtable metadict (metadata modis)]
    (map #(.get metadict %) meta-keys)))

;; From [NASA's MODIS information](http://goo.gl/C28fG), "The MODLAND
;; Tile ID is an 8 digit integer, such as 51018009, that is used to
;; specify a gridded product's projection, tile size and horizontal
;; and vertical tile number."
;;
;; The first digit is used to identify the projection, while the
;; second digit is used to specify the tile size. 1 is 1km data (full
;; tile size) , 2 is quarter size (500m data), and 4 is 16th tile
;; size, or 250m data. REDD deals with MODIS products from collection
;; 5, which uses a sinusoial projection.
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
  "Returns a sequence containing the resolution and a tilestring,
  formatted as 'hhhvvv', where h and v refer to the MODIS tile
  referenced by the supplied TileID. The precondition makes sure that
  the processed product uses a sinusoial projection."
  [tileid]
  {:pre [(= (first tileid) \5)]}
  ((juxt tileid->res
         #(subs % 2)) tileid))

(defn t-res
  "Returns the temporal resolution of the supplied MODIS short product
  name. (wrapper exists for cascalog. Better to use the `temporal-res`
  map as a function, but this unsupported."
  [prodname]
  (temporal-res prodname))

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
  [source datasets chunk-size]
  (let [keys ["SHORTNAME" "TileID" "RANGEBEGINNINGDATE"]]
    (<- [?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?int-chunk]
        (source ?filename ?hdf)
        (unpack-modis [datasets] ?hdf :> ?dataset ?freetile)
        (raster-chunks [chunk-size] ?freetile :> ?chunkid ?chunk)
        (meta-values [keys] ?freetile :> ?productname ?tileid ?date)
        (t-res ?productname :> ?temporal-res)
        (split-id ?tileid :> ?spatial-res ?tilestring)
        (int-array ?chunk :> ?int-chunk)
        (:distinct false))))