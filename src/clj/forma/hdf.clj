(ns forma.hdf
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]
           [org.apache.hadoop.io BytesWritable])
  (:require [cascalog [ops :as c] [workflow :as w] [io :as io]]
            [clojure.contrib [math :as m] [io :as i]]))

(defn get-metadata
  "Takes in a decompressed HDF file and a key, and returns the associated
   metadata.
   [TODO] takes in a seq of metadata, returns a seq of values."
  [hdf key]
  (let [metadata (.GetMetadata_Dict hdf "")]
    (.get metadata key)))

(defn get-tileid
  "Returns the TileID of a decompressed MODIS file."
  [hdf]
  (get-metadata hdf "TileID"))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

(defmapop unpack
  "Stateful approach to unpacking HDF files. Registers all gdal formats,
   Creates a temp directory, then saves the byte array to disk. This byte
   array is processed with gdal, and then the temp directory is destroyed.
   Function returns the decompressed MODIS file as a 1-tuple."
  {:stateful true}
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

(defn unpack-hdf
  "My first try at unpacking HDF files. This works, but doesn't clean up
   after decompression. See the defmapop for a better option."
  [bytestream]
  (let [hash (Integer/toString (m/abs (.hashCode bytestream)))
        tfile (i/file (io/temp-dir "hdf") hash)
        tpath (.toString tfile)]
    (do
      (gdal/AllRegister)
      (i/copy (get-bytes bytestream) tfile)
      (gdal/Open tpath 0))))