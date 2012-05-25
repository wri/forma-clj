(ns forma.schema-test
  (:use forma.schema
        [midje sweet cascalog])
  (:require [forma.date-time :as date])
  (:import [forma.schema
            ArrayValue DataChunk DataValue DoubleArray FireArray
            FireValue FormaValue IntArray LocationProperty
            LocationPropertyValue LongArray ModisChunkLocation
            ModisPixelLocation ShortArray TimeSeries FormaArray
            NeighborValue]
           [org.apache.thrift TBase TUnion]))

;; ## Various schema tests

(def neighbors
  [(forma-value nil 1 1 1 1)
   (forma-value (fire-value 1 1 1 1) 2 2 2 2)])

(fact
  "Checks that neighbors are being combined properly."
  (let [test-seq [(forma-value nil 1 1 1 1) (forma-value nil 2 2 2 2)]]
    (combine-neighbors test-seq) => (neighbor-value (fire-value 0 0 0 0)
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
                       (timeseries-value (+ f-start offset)
                                         (mk-array-value (FireArray.
                                                          [(fire-value 0 0 0 1)
                                                           (fire-value 1 1 1 1)]))))]
     (adjust-fires est-map (mk-f-series ?offset)) => [?series]))
 ?offset ?series
 0       (timeseries-value  f-start (mk-array-value (FireArray.
                                                     [(fire-value 0 0 0 1)
                                                      (fire-value 1 1 1 1)])))
 1       (timeseries-value f-start (mk-array-value (FireArray.
                                                    [(fire-value 0 0 0 1)])))
 2       nil)

(fact "chunk to pixel location conversions."
  (let [pix-loc (-> (chunk-location "1000" 10 10 59 24000)
                    (chunkloc->pixloc 23999))]
    pix-loc => (pixel-location "1000" 10 10 1199 1199)))

(tabular
 (fact "Test Thriftable protocol"
   (get-vals ?x) => ?vals)
 ?x ?vals
 (TimeSeries. 0 1 (mk-array-value (DoubleArray. [1 2 3]))) [1 2 3]
 (FormaArray. [(FormaValue. (FireValue. 1 1 1 1) 0 0 0)]) [(FormaValue. (FireValue. 1 1 1 1) 0 0 0)]
 (ShortArray. [1 2 3]) [1 2 3]
 (IntArray. [1 2 3]) [1 2 3]
 (LongArray. [1 2 3]) [1 2 3]
 (DoubleArray. [1 2 3]) [1 2 3]
 (FireArray. [1 2 3]) [1 2 3]
 (mk-array-value (DoubleArray. [1 2 3])) [1 2 3])

(fact "Test chunk-locval method."
  (let [loc (LocationProperty. 
             (LocationPropertyValue/chunkLocation
              (ModisChunkLocation. "500" 28 8 1 24000)))
        val (DataValue/vals (ArrayValue/ints (IntArray. [1 1 1 1])))
        dc (DataChunk. "vcf" loc val "16")]
    (chunk-locval dc) => [loc [1 1 1 1]]))

(fact "Test pixel-prop-location method."
  (let [modis-pixel (ModisPixelLocation. "500" 28 8 1 10)
        val (LocationPropertyValue/pixelLocation modis-pixel)
        locprop (LocationProperty. val)]
    (pixel-prop-location locprop) => modis-pixel))
