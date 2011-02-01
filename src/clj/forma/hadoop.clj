(ns forma.hadoop
  (:import [org.apache.hadoop.io BytesWritable]))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

