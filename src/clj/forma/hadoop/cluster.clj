;; Very much a work in progress...
;;
;; TODO: Get pallet cluster configuration stuff into here, so we can
;; use job specific cluster definitions.

(ns forma.hadoop.cluster
  (:use [clojure.string :only (join)])
  (:import [forma FloatsSerialization IntsSerialization]
           [backtype.hadoop ThriftSerialization]
           [cascading.tuple.hadoop BytesSerialization TupleSerialization]
           [org.apache.hadoop.io.serializer WritableSerialization JavaSerialization]))

;; Returns a sequence of the classnames of all serializers used in
;; FORMA.
;;
;; TODO: make sure these are in the proper order.
(def serializers
  (let [class-seq (for [cls [ThriftSerialization FloatsSerialization
                             IntsSerialization BytesSerialization
                             TupleSerialization WritableSerialization
                             JavaSerialization]]
                    (.getName cls))]
    (join "," class-seq)))

(def forma-config
  {:core-site {:io.serializations serializers}})
