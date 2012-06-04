(ns forma.schema-test
  (:use forma.schema
        [midje sweet cascalog])
  (:require [forma.date-time :as date]
            [forma.thrift :as thrift])
  (:import [forma.schema
            ArrayValue DataChunk DataValue DoubleArray FireArray
            FireValue FormaValue IntArray LocationProperty
            LocationPropertyValue LongArray ModisChunkLocation
            ModisPixelLocation ShortArray TimeSeries FormaArray
            NeighborValue]
           [org.apache.thrift TBase TUnion]))

;; ## Various schema tests

(def neighbors
  [(thrift/FormaValue* nil 1 1 1 1)
   (thrift/FormaValue* (thrift/FireValue* 1 1 1 1) 2 2 2 2)])

(fact
  "Checks that neighbors are being combined properly."
  (let [test-seq [(thrift/FormaValue* nil 1 1 1 1)
                  (thrift/FormaValue* nil 2 2 2 2)]]
    (combine-neighbors test-seq) => (neighbor-value (thrift/FireValue* 0 0 0 0)
                                                    2
                                                    1.5 1
                                                    1.5 1
                                                    1.5 1
                                                    1.5 1)))

(fact "boundaries testing."
  (boundaries [ 0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]
  (boundaries [ 0 [1 2 3 4] [2 3 4 5]]) => (throws AssertionError))

(tabular
 (fact "sequence adjustment testing."
   (adjust ?a0 ?a-vec ?b0 ?b-vec) => [?start ?a ?b])
 ?a0 ?a-vec    ?b0 ?b-vec    ?start ?a      ?b
 0   [1 2 3 4] 1   [2 3 4 5] 1      [2 3 4] [2 3 4]
 2   [9 8 7]   0   [1 2 3 4] 2      [9 8]   [3 4]
 10  [2 3 4]   1   [1 2 3]   10     []      [])

(tabular
 (facts "adjust-fires testing."
   (let [est-map {:est-start "2005-01-01"
                  :est-end "2005-02-01"
                  :t-res "32"}
         f-start (date/datetime->period "32" "2005-01-01")
         mk-f-series (fn [offset]
                       (thrift/TimeSeries* (+ f-start offset)
                                           [(thrift/FireValue* 0 0 0 1)
                                            (thrift/FireValue* 1 1 1 1)]))]
     (adjust-fires est-map (mk-f-series ?offset)) => [?series]
     ))
 ?offset ?series
 0       (thrift/TimeSeries*  f-start [(thrift/FireValue* 0 0 0 1)
                                        (thrift/FireValue* 1 1 1 1)])
 1       (thrift/TimeSeries* f-start [[(thrift/FireValue* 0 0 0 1)]])
;; 2       nil
 )

