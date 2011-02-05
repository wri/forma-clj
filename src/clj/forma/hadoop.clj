(ns forma.hadoop
  (:use (clojure.contrib [math :only (abs)]))
  (:import [org.apache.hadoop.io BytesWritable]))

(defn hash-str
  "Generates a unique identifier for the supplied BytesWritable
  object. Useful as a filename, when worried about clashes."
  [^BytesWritable bytes]
  (str (abs (.hashCode bytes))))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object. As
  mentioned in the Hadoop documentation, only the first N bytes are
  good, where N is number returned by a call to getLength."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))