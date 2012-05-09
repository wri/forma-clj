(ns forma.schema-test
  (:use forma.schema
        [midje sweet cascalog])
  (:require [forma.date-time :as date]))

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
 (fact "adjust and adjust-timeseries testing, combined!"
   (adjust ?a0 ?a-vec ?b0 ?b-vec) => [?start ?a ?b]
   (adjust-timeseries (ts-record ?a0 ?a-vec)
                      (ts-record ?b0 ?b-vec))
   => [(ts-record ?start ?a)
       (ts-record ?start ?b)])
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
                                         [(fire-value 0 0 0 1)
                                          (fire-value 1 1 1 1)]))]
     (adjust-fires est-map (mk-f-series ?offset)) => [?series]))
 ?offset ?series
 0       (timeseries-value  f-start [(fire-value 0 0 0 1) (fire-value 1 1 1 1)])
 1       (timeseries-value f-start [(fire-value 0 0 0 1)])
 2       nil)

(fact "chunk to pixel location conversions."
  (let [pix-loc (-> (chunk-location "1000" 10 10 59 24000)
                    (chunkloc->pixloc 23999))]
    pix-loc => (pixel-location "1000" 10 10 1199 1199)))


;; tests of thrift schema

(def test-obj
  "Create a basic thrift object"
  (mk-short-data-chunk "ndvi" "500" "16" 28 8 0 0 "2000-01-01" [1 2 3]))

(tabular
 (fact
   "Test whether thrift object was created correctly"
   (?method test-obj) => ?result)
 ?method ?result
 .date "2000-01-01"
 .temporalRes "16"
 .dataset "ndvi")

(tabular
 (fact
  "Test getting short vector out of thrift object. Also checks that the vector is actually filled with shorts"
  (?func test-obj) => ?result)
 ?func ?result
  get-short-vec [1 2 3]
  (comp type first get-short-vec) java.lang.Short)

(fact
  "Test extracting location info from thrift object"
  (get-location-properties test-obj) => ["500" 28 8 0 0])

(fact
  "Test full unpacking of DataChunk object into our standard location
   + series format"
  (unpack-data-chunk-short test-obj) => ["500" 28 8 0 0 690 [1 2 3]])