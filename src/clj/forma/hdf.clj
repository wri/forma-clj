(ns forma.hdf
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset Band]
           [org.gdal.gdalconst gdalconstConstants]
           [org.apache.hadoop.io BytesWritable])
  (:require [cascalog [ops :as c] [workflow :as w] [io :as io]]
            [clojure.contrib [math :as m] [io :as i] [string :as s]]))

;; ##Constants

(def
  #^{:doc "Map between symbols and chosen substrings of
MODIS subdataset keys."}
  *modis-subsets*
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
  *forma-subsets*
  #{:ndvi :evi :qual :reli})

(def
  #^{:doc "Arbitrary number of pixels in a chunk of MODIS data."}
  *chunk-size*
  24000)

;; ##Helper Functions
;; These are organized fairly well... more docs later.

(defn metadata
  "Returns the metadata hashtable for the supplied (opened) MODIS file."
  [^Dataset modis arg]
  (.GetMetadata_Dict modis arg))

(defn subdataset-key
  "For a given Hashtable entry, returns the associated key in *modis-subsets*."
  [entry]
  (let [val (.getValue entry)
        key (.getKey entry)]
    (some #(if (s/substring? (% *modis-subsets*) val) %)
          (keys *modis-subsets*))))

(defn subdataset-filter
  "Generates a predicate function that compares Hashtable entries to a supplied
   set of acceptable keys. These keys should exist in *modis-subsets*."
  [good-keys]
  (let [kept-substrings (map *modis-subsets* good-keys)]
    (fn [entry]
      (let [val (.getValue entry)
            key (.getKey entry)]
        (and (s/substring? "_NAME" key)
             (some #(s/substring? % val) kept-substrings))))))

(def forma-dataset? (subdataset-filter *forma-subsets*))

(defn make-subdataset
  "Accepts an entry in the SUBDATASETS Hashtable of a MODIS Dataset, and returns
   a 2-tuple with the forma dataset key and the Dataset object."
  [entry]
  (vector (subdataset-key entry) (gdal/Open (.getValue entry))))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

(defn subdatasets
  "Returns the SUBDATASETS metadata Hashtable for a given filepath."
  [hdf-file]
  (let [path (.toString hdf-file)]
    (metadata (gdal/Open path) "SUBDATASETS")))

(defn raster-array
  "Unpacks the data inside of a MODIS band into a 1xN integer array."
  [^Dataset data]
  (let [band (.GetRasterBand data 1)
        type (gdalconstConstants/GDT_UInt16)
        width (.GetXSize band)
        height (.GetYSize band)
        ret (int-array (* width height))]
    (do (.ReadRaster band 0 0 width height type ret)
        ret)))

;; ##Conversion between MODIS and mercator.
(defn modis-position
  "For a given MODIS chunk and index within that chunk, returns [line, sample] within the MODIS tile."
  [chunk index]
  (let [line (* chunk *chunk-size*)
        sample (+ line index)]
    (vector line sample)))

;; ##Cascalog Custom Functions
;; These guys actually get called by the queries inside of core.

(defmapop
  #^{:doc "Generates metadata values for a given unpacked MODIS
Dataset and a seq of keys."}
  [meta-values [meta-keys]] [modis]
  (let [metadata (metadata modis "")]
    (map #(.get metadata %) meta-keys)))

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
       (io/temp-dir "hdf")))
  ([tdir stream]
     (let [hash (Integer/toString (m/abs (.hashCode stream)))
           tfile (i/file tdir hash)
           bytes (get-bytes stream)]
       (do
         (i/copy (get-bytes stream) tfile)
         (map make-subdataset
              (filter forma-dataset?
                      (subdatasets tfile))))))
  ([tdir]
     (i/delete-file-recursively tdir)))