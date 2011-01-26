(ns forma.hdf
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]
           [org.apache.hadoop.io BytesWritable])
  (:require [cascalog [ops :as c] [workflow :as w] [io :as io]]
            [clojure.contrib [math :as m] [io :as i] [string :as s]]))


;; ## Attempts to Get Metadata

;; Requires an unpacked MODIS file and a seq of meta-data keys.
(defmapop [meta-values [meta-keys]] [modis]
  (let [metadata (.GetMetadata_Dict modis "")]
    (map #(.get metadata %) meta-keys)))

(defn get-metadata
  "Takes in a decompressed HDF file and a key, and returns the associated
   metadata."
  [hdf keys]
  (let [metadata (.GetMetadata_Dict hdf "")]
    (map #(.get metadata %) keys)))

(defn get-tileid
  "Returns the TileID of a decompressed MODIS file."
  [hdf]
  (get-metadata hdf ["TileID"]))


;; ## Mapping operations

;; Stateful approach to unpacking HDF files. Registers all gdal formats,
;; Creates a temp directory, then saves the byte array to disk. This byte
;; array is processed with gdal, and then the temp directory is destroyed.
;; Function returns the decompressed MODIS file as a 1-tuple.

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

(defmapop unpack {:stateful true}
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
         (gdal/Open (.toString tfile) 0)))) ;; this value should return a seq instea
  ([tdir]
     (i/delete-file-recursively tdir)))

(def *modis-subsets*
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

(def forma-subsets
  #{:ndvi :evi :qual :reli})

(defn subdata-key [entry]
  (let [val (.getValue entry)
        key (.getKey entry)]
    (some #(if (s/substring? (% *modis-subsets*) val)
             %)
          forma-sets)))

(defn subdataset-filter
  "Generates a predicate function that compares Hashtable entries to a supplied
   list of acceptable keys. These keys should exist in *modis-subsets*."
  [to-keep]
  (fn [entry]
    (let [val (.getValue entry)
        key (.getKey entry)]
    (and (s/substring? "_NAME" key)
         (some #(s/substring? % val)
               (map *modis-subsets* to-keep))))))

(def forma-dataset? (subdataset-filter forma-subsets))

(defn make-subdataset
  "Accepts an entry in the SUBDATASETS Hashtable of a MODIS Dataset, and returns
   a 2-tuple with the forma dataset key and the Dataset object."
  [entry]
  (vector (subdata-key entry) (gdal/Open (.getValue entry))))

;; (let [metadata (.GetMetadata_Dict opened-file "SUBDATASETS")]
;;   (map make-subdataset (filter forma-dataset? metadata)))