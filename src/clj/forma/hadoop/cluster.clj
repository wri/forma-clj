;; Very much a work in progress...
;;
;; TODO: Get pallet cluster configuration stuff into here, so we can
;; use job specific cluster definitions.

(ns forma.hadoop.cluster
  (:import [forma FloatsSerialization IntsSerialization]
           [cascading.tuple.hadoop BytesSerialization TupleSerialization]
           [org.apache.hadoop.io.serializer WritableSerialization JavaSerialization]))

;; Returns a sequence of the classnames of all serializers used in
;; Forma.
;;
;; TODO: make sure these are in the proper order.

(def serializers
  (let [class-seq (for [cls [FloatsSerialization IntsSerialization
                             BytesSerialization TupleSerialization
                             WritableSerialization JavaSerialization]]
                    (.getName cls))]
    (apply str (interpose "," class-seq))))

(def forma-config
  {:core-site {:io.serializations serializers}})
