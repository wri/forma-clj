(ns forma.hadoop.cluster
  (:import [forma FloatsSerialization IntsSerialization]
           [cascading.tuple.hadoop BytesSerialization TupleSerialization]
           [org.apache.hadoop.io.serializer WritableSerialization JavaSerialization]))

;; Nothing here, yet!
;;
;; Returns a sequence of the classnames of all serializers used in
;; Forma.

(def serializers
  (let [class-seq (for [cls [FloatsSerialization IntsSerialization
                             BytesSerialization TupleSerialization
                             WritableSerialization JavaSerialization]]
                    (.getName cls))]
    (apply str (interpose "," class-seq))))

(def forma-config
  {:core-site {:io.serializations serializers}})
