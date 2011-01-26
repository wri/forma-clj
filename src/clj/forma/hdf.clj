(ns forma.hdf
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]
           [org.apache.hadoop.io BytesWritable])
  (:require [cascalog [ops :as c] [workflow :as w] [io :as io]]
            [clojure.contrib [math :as m] [io :as i]]))

(defn get-metadata
  "Takes in a decompressed HDF file and a key, and returns the associated
   metadata."
  [hdf keys]
  (let [metadata (.GetMetadata_Dict hdf "")]
    (vec (map #(.get metadata %) keys))))

(defn get-tileid
  "Returns the TileID of a decompressed MODIS file."
  [hdf]
  (get-metadata hdf ["TileID"]))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))


;; Requires an unpacked MODIS file and a seq of meta-data keys.

(defmapop [meta-values [meta-keys]] [modis]
  (let [metadata (.GetMetadata_Dict modis "")]
    (vec (map #(.get metadata %) meta-keys))))

;; ## Mapping operations

;; Stateful approach to unpacking HDF files. Registers all gdal formats,
;; Creates a temp directory, then saves the byte array to disk. This byte
;; array is processed with gdal, and then the temp directory is destroyed.
;; Function returns the decompressed MODIS file as a 1-tuple.

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
         (gdal/Open (.toString tfile) 0))))
  ([tdir]
     (i/delete-file-recursively tdir)))