(ns forma.hdf
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]
           [org.apache.hadoop.io BytesWritable])
  (:require [cascalog [ops :as c] [workflow :as w] [io :as io]]
            [clojure.contrib [math :as m] [io :as i]]))

(def nasa-test-file "/Users/sritchie/Desktop/MODIS/MOD13A3.A2000032.h05v11.005.2006271174501.hdf")

(defn get-metadata
  "Right now, just takes a single key.
   [TODO] takes in a seq of metadata, returns a seq of values."
  [hdf key]
  (let [metadata (.GetMetadata_Dict hdf "")]
    (.get metadata key)))

(defn get-tileid [hdf]
  (get-metadata hdf "TileID"))

(def nasa-dir-path
  "/Users/sritchie/Desktop/MODISTEST/")

(defmapop [unpack [somearg]] {:stateful true} 
  ([]
     (let [hash (Integer/toString (m/abs (.hashCode key)))]
       (do (gdal/AllRegister)
           (i/file (io/temp-dir "hdf") hash))))
  ([tfile stream]
     (let [tpath (.toString tfile)
           bytes (byte-array (.getLength stream)
                             (.getBytes stream))]
       (do
         (i/copy bytes tfile)
         (gdal/Open tpath 0)))) 
  ([tfile]
     (i/delete-file tfile)))

(defn get-bytes [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

(defn unpack-hdf
  [bytestream]
  (let [hash (Integer/toString (m/abs (.hashCode bytestream)))
        tfile (i/file (io/temp-dir "hdf") hash)
        tpath (.toString tfile)]
    (do
      (gdal/AllRegister)
      (i/copy (get-bytes bytestream) tfile)
      (gdal/Open tpath 0))))