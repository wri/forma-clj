(ns forma.hdf
  (:use (cascalog [api :only (defmapop defmapcatop)]
                  [io :only (temp-dir)])
        (forma [hadoop :only (get-bytes)])
        (clojure.contrib [math :only (abs)]
                         [io :only (file copy delete-file-recursively)]
                         [string :only (substring? as-str)]))
  (:import [java.util Map$Entry Hashtable]
           [java.io File]
           [org.gdal.gdal gdal Dataset Band]
           [org.gdal.gdalconst gdalconstConstants]
           [org.apache.hadoop.io BytesWritable]))

(set! *warn-on-reflection* true)

;; ##Constants

(def
  #^{:doc "Map between symbols and chosen substrings of
MODIS subdataset keys."}
  modis-subsets
  {:evi "monthly EVI"
   :qual "VI Quality"
   :red "red reflectance"
   :nir "NIR reflectance"
   :azi "azimuth"
   :blue "blue reflectance"
   :reli "monthly pixel"
   :viewz "view zenith"
   :ndvi "NDVI"
   :sunz "sun zenith"})

(def
  #^{:doc "Default set of data for FORMA processing."}
  forma-subsets
  #{:ndvi :evi :qual :reli})

(def
  #^{:doc "Arbitrary number of pixels in a chunk of MODIS data."}
  chunk-size
  24000)

;; ##Helper Functions
;; These are organized fairly well... more docs later.

(defn metadata
  "Returns the metadata hashtable for the supplied (opened) MODIS file."
  ([^Dataset modis arg]
     (.GetMetadata_Dict modis arg))
  ([modis]
     (metadata modis "")))

(defn lookup
  "Looks up a key in the supplied hashtable."
  [key ^Hashtable table]
  (.get table key))

(defn split-entry
  "Splits a given Entry into its map and key."
  [^Map$Entry entry]
  (vector (.getValue entry) (.getKey entry)))

(defn subdataset-key
  "For a given Hashtable value, returns the associated key in
  modis-subsets."
  [val]
  (some #(if (substring? (% modis-subsets) val) (as-str %))
          (keys modis-subsets)))

(defn subdataset-filter
  "Generates a predicate function that compares Hashtable entries to a
   supplied set of acceptable keys. These keys should exist in
   modis-subsets."
  [good-keys]
  (let [kept-substrings (map modis-subsets good-keys)]
    (fn [entry]
      (let [[val key] (split-entry entry)]
        (and (substring? "_NAME" key)
             (some #(substring? % val) kept-substrings))))))

(def forma-dataset? (subdataset-filter forma-subsets))

(defn make-subdataset
  "Accepts an entry in the SUBDATASETS Hashtable of a MODIS Dataset,
   and returns a 2-tuple with the forma dataset key and the Dataset
   object."
  [entry]
  (let [[path] (split-entry entry)]
    (vector (subdataset-key path) (gdal/Open path))))

(defn subdatasets
  "Returns the SUBDATASETS metadata Hashtable for a given filepath."
  [hdf-file]
  (let [path (str hdf-file)]
    (metadata (gdal/Open path) "SUBDATASETS")))

(defn raster-array
  "Unpacks the data inside of a MODIS band into a 1xN integer array."
  [^Dataset data]
  (let [^Band band (.GetRasterBand data 1)
        type (gdalconstConstants/GDT_UInt16)
        width (.GetXSize band)
        height (.GetYSize band)
        ret (int-array (* width height))]
    (do (.ReadRaster band 0 0 width height type ret) ret)))

(defn tile-position
  "For a given MODIS chunk and index within that chunk, returns [line,
  sample] within the MODIS tile."
  [chunk index]
  (let [line (* chunk chunk-size)
        sample (+ line index)]
    (vector line sample)))

(defn split-id
  "Splits the TileID metadata string into integer x and y components."
  [tileid]
  (let [[_ _ tile-x tile-y] (re-find #"(\d{2})(\d{3})(\d{3})" tileid)]
    (map #(Integer/parseInt %) [tile-x tile-y])))

;; ##Cascalog Custom Functions
;;These guys actually get called by the queries inside of core.

(defmapop
  #^{:doc "Generates metadata values for a given unpacked MODIS
Dataset and a seq of keys."}
  [meta-values [meta-keys]] [modis]
  (let [metadict (metadata modis)]
    (map #(lookup % metadict) meta-keys)))

(defmapcatop
  #^{:doc "Stateful approach to unpacking HDF files. Registers all
gdal formats, Creates a temp directory, then saves the byte array to
disk. This byte array is processed with gdal, and then the temp
directory is destroyed. Function returns the decompressed MODIS file
as a 1-tuple."}
  unpack {:stateful true}
  ([]
     (do
       (gdal/AllRegister)
       (temp-dir "hdf")))
  ([tdir ^BytesWritable stream]
     (let [hash (str (abs (.hashCode stream)))
           tfile (file tdir hash)
           bytes (get-bytes stream)]
       (do
         (copy (get-bytes stream) tfile)
         (map make-subdataset
              (filter forma-dataset?
                      (subdatasets tfile))))))
  ([tdir]
     (delete-file-recursively tdir)))
