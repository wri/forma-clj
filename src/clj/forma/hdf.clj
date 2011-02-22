;; Documentation, again. What's the point of these guys? This lets us
;; unpack a MODIS hdf file into the proper format.

(ns forma.hdf
  (:use (forma hadoop [conversion :only (datetime->period)])
        (cascalog api [io :only (temp-dir)])
        (clojure.contrib [seq-utils :only (find-first indexed)]))
  (:require (clojure.contrib [string :as s]
                             [io :as io]))
  (:import [java.util Hashtable]
           [java.io File]
           [org.gdal.gdal gdal Dataset Band]
           [org.gdal.gdalconst gdalconstConstants]))

;; TODO document:
;; http://tbrs.arizona.edu/project/MODIS/MOD13.C5-UsersGuide-HTML-v1.00/sect0005.html
;; The terra products use regular production, while the aqua products
;; use phased production.

;; ## Constants

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

;; ## MODIS Unpacking

(defn metadata
  "Returns the metadata hashtable for the supplied MODIS Dataset."
  ([^Dataset modis key]
     (.GetMetadata_Dict modis key))
  ([modis]
     (metadata modis "")))


;; Every MODIS file (archived in HDF4) packages a number of different
;; datasets; 9, in the case of the MOD13A3 product. The paths to these
;; are contained within the "SUBDATASETS" metadata dictionary of the
;; wrapping archive. This dictionary contains 18 groups of K-V pairs,
;; or 2 per dataset:
;;
;; Key: SUBDATASET_2_NAME
;; Val: HDF4_EOS:EOS_GRID:"/path/to/modis.hdf":MOD_Grid_monthly_1km_VI:1 km
;; monthly EVI
;; Key: SUBDATASET_4_DESC
;; Val: [1200x1200] 1 km monthly EVI MOD_Grid_monthly_1km_VI (16-bit
;; integer)
;;
;; We're only interested in the names, as these alone allow us to
;; uniquely identify each dataset.

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

;; ##Cascalog Custom Functions

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

;; ## Chunking Functions

(defmapcatop [raster-chunks [chunk-size]]
  ^{:doc "Unpacks the data inside of a MODIS band and partitions it
  into chunks sized according to the supplied value. Specifically,
  returns a lazy sequence of 2-tuples a lazy sequence of 2-tuples of
  the form (chunk-index, int-array)."}
  [^Dataset data]
  (let [^Band band (.GetRasterBand data 1)
        width (.GetXSize band)
        height (.GetYSize band)
        ret (int-array (* width height))]
    (.ReadRaster band 0 0 width height ret)
    (indexed (partition chunk-size ret))))

;; ## Metadata Parsing

(defmapop [meta-values [meta-keys]]
  ^{:doc "Returns metadata values for a given unpacked MODIS Dataset
and a seq of keys."}
  [modis]
  (let [^Hashtable metadict (metadata modis)]
    (map #(.get metadict %) meta-keys)))

;; TODO -- add some good documentation here about what the next
;; section is accomplishing. I want to check marginalia to see the
;; best way to do this stuff. We might actually want to break this out
;; into its own file.

;; It's described here:
;; http://modis-250m.nascom.nasa.gov/developers/tileid.html

;; The first digit is used to identify the projection, while the
;; second digit is used to specify the tile size. 1 is 1km data (full
;; tile size) , 2 is quarter size (500m data), and 4 is 16th tile
;; size, or 250m data.

(defn parse-ints
  "Converts all strings in the supplied collection to their integer
  representations."
  [coll]
  (map #(Integer/parseInt %) coll))

(def
  ^{:doc "Map between the second digit in a MODIS TileID metadata
value and the corresponding resolution of the tile data."}
  res-map
  {:1 "1000"
   :2 "500"
   :4 "250"})

(defn tileid->res
  "Returns a string representation of the resolution (in meters) of
the tile data referenced by the supplied TileID.The second character
of a MODIS TileID acts as a key to retrieve this data."
  [tileid]
  (res-map (keyword (subs tileid 1 2))))

(defn tileid->xy
  "Extracts integer representations of the MODIS X and Y coordinates
referenced by the supplied MODIS TileID."
  [tileid]
  (parse-ints
   (map (partial apply str)
        (partition 3 (subs tileid 2)))))

(defn split-id
  "Returns a sequence containing the resolution, X and Y
  coordinates (on the MODIS grid) referenced by the supplied MODIS
  TileID."
  [tileid]
  (flatten
   ((juxt tileid->res
          tileid->xy) tileid)))

;; ## Period Parser

;; TODO -- better docs.
(defmapop to-period [res date]
  (apply datetime->period
         res
         (parse-ints (re-seq #"\d+" date))))

;; ## The Chunker!

(defn modis-chunks
  "Takes a source, either (hfs-wholefile dir) or (hfs-seqfile dir),
  and returns a number of 7-tuples that fully describe chunks of MODIS
  data for the supplied datasets. This function currently can't be
  refactored, as the gdal.Dataset object doesn't implement
  Serializable. This forces us to completely tease apart the HDF files
  at the source before we can return tuples from the subquery. Chunks
  are represented as seqs of floats. Be sure to convert to vector
  before running any sort of data analysis, as (seqs require linear
  time for lookups)."
  [source datasets chunk-size]
  (let [keys ["TileID" "RANGEBEGINNINGDATE"]]
    (<- [?dataset ?res ?tile-h ?tile-v ?period ?chunkid ?chunk]
        (source ?filename ?hdf)
        (unpack-modis [datasets] ?hdf :> ?dataset ?freetile)
        (raster-chunks [chunk-size] ?freetile :> ?chunkid ?chunk)
        (meta-values [keys] ?freetile :> ?tileid ?datestring)
        (split-id ?tileid :> ?res ?tile-h ?tile-v)
        (to-period ?res ?datestring :> ?period)
        (:distinct false))))