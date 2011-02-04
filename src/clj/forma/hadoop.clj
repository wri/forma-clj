(ns forma.hadoop
  (:import [org.apache.hadoop.io BytesWritable]))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object. As
  mentioned in the Hadoop documentation, only the first N bytes are
  good, where N is number returned by a call to getLength."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))