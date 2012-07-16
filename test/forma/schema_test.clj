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
  [(thrift/FormaValue* (thrift/FireValue* 1 1 1 1) 1. 1. 1. 1.)
   (thrift/FormaValue* (thrift/FireValue* 2 1 1 2) 2. 2. 2. 2.)])

(fact
  "Checks that neighbors are being combined properly."
  (combine-neighbors neighbors) => (neighbor-value (thrift/FireValue* 3 2 2 3)
                                                   2
                                                   1.5 1.
                                                   1.5 1.
                                                   1.5 1.
                                                   1.5 1.))

;; (fact "boundaries testing."
;;   (boundaries [ 0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]
;;   (boundaries [ 0 [1 2 3 4] [2 3 4 5]]) => (throws AssertionError))

;; (tabular
;;  (fact "sequence adjustment testing."
;;    (adjust ?a0 ?a-vec ?b0 ?b-vec) => [?start ?a ?b])
;;  ?a0 ?a-vec    ?b0 ?b-vec    ?start ?a      ?b
;;  0   [1 2 3 4] 1   [2 3 4 5] 1      [2 3 4] [2 3 4]
;;  2   [9 8 7]   0   [1 2 3 4] 2      [9 8]   [3 4]
;;  10  [2 3 4]   1   [1 2 3]   10     []      [])

;; (tabular
;;  (facts "adjust-fires testing."
;;    (let [est-map {:est-start "2005-01-01"
;;                   :est-end "2005-02-01"
;;                   :t-res "32"}
;;          f-start (date/datetime->period "32" "2005-01-01")
;;          mk-f-series (fn [offset]
;;                        (thrift/TimeSeries* (+ f-start offset)
;;                                            [(thrift/FireValue* 0 0 0 1)
;;                                             (thrift/FireValue* 1 1 1 1)]))]
;;      (adjust-fires est-map (mk-f-series ?offset)) => [?series]
;;      ))
;;  ?offset ?series
;;  0       (thrift/TimeSeries*  f-start [(thrift/FireValue* 0 0 0 1)
;;                                         (thrift/FireValue* 1 1 1 1)])
;;  1       (thrift/TimeSeries* f-start [[(thrift/FireValue* 0 0 0 1)]])
;;  2       nil)

;; (let [fires (thrift/TimeSeries* 0 [(thrift/FireValue* 0 5 0 0)
;;                                    (thrift/FireValue* 0 0 0 0)])
;;       forma-val-1 [[(thrift/FireValue* 0 5 0 0) 1 4 7 1]
;;                    [(thrift/FireValue* 0 0 0 0) 2 5 8 1]]
;;       forma-val-2 [[(thrift/FireValue* 0 0 0 0) 1 4 7 1]
;;                    [(thrift/FireValue* 0 0 0 0) 2 5 8 1]]]
;;   (tabular
;;    (fact
;;      (forma-seq ?fires ?short ?long ?t-stat ?break)  => ?result)
;;    ?fires ?short ?long ?t-stat ?break ?result
;;    fires [1 2] [4 5] [7 8] [1 1] [forma-val-1]
;;    nil [1 2] [4 5] [7 8] [1 1] [forma-val-2]))


;; (let [fires (thrift/TimeSeries* 0 [(thrift/FireValue* 0 5 0 0)
;;                                    (thrift/FireValue* 0 0 0 0)])
;;       forma-val-1 [[(thrift/FireValue* 0 5 0 0) 1 4 7 1]
;;                    [(thrift/FireValue* 0 0 0 0) 2 5 8 1]]
;;       forma-val-2 [[(thrift/FireValue* 0 0 0 0) 1 4 7 1]
;;                    [(thrift/FireValue* 0 0 0 0) 2 5 8 1]]]
;;   (tabular
;;    (fact
;;      (forma-seq ?fires ?short ?long ?t-stat ?break)  => ?result)
;;    ?fires ?short ?long ?t-stat ?break ?result
;;    fires [1 2] [4 5] [7 8] [1 1] [forma-val-1]
;;    nil [1 2] [4 5] [7 8] [1 1] [forma-val-2]))


;; (let [forma-val-1 [[(thrift/FireValue* 0 0 0 0) 1 4 7 1]
;;                    [(thrift/FireValue* 0 0 0 0) -1 5 8 3]
;;                    [(thrift/FireValue* 0 0 0 0) -1 5 8 3]]]
;;   (tabular
;;    (fact
;;      (forma-seq-non-thrift ?fires ?short ?long ?t-stat ?break)  => ?result)
;;    ?fires ?short ?long ?t-stat ?break ?result
;;    nil [1 -1 2] [4 5 5] [7 8 8] [1 3 2] [forma-val-1]))

;; (fact "Test pixel-prop-location method."
;;   (let [modis-pixel (ModisPixelLocation. "500" 28 8 1 10)
;;         val (LocationPropertyValue/pixelLocation modis-pixel)
;;         locprop (LocationProperty. val)]
;;     (pixel-prop-location locprop) => modis-pixel))

;; (fact
;;   "Test vector wrapping of get-vals"
;;   (let [arr (mk-array-value (DoubleArray. [1 2 3]))]
;;     (get-vals-wrap arr)) => [[1 2 3]])
