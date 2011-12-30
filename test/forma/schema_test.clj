(ns forma.schema-test
  (:use forma.schema
        [midje sweet cascalog])
  (:require [forma.date-time :as date]))

;; ## Various schema tests

(def neighbors
  [(forma-value nil 1 1 1)
   (forma-value (fire-value 1 1 1 1) 2 2 2)])

(fact
  "Tests that the combine neighbors function produces the proper
textual representation."
  (let [s "0\t0\t0\t0\t1\t1\t1\t1\t1\t1\t1\t2\t1.5\t1\t1.5\t1\t1.5\t1"]
    (textify (first neighbors)
             (combine-neighbors neighbors)) => s))

(fact
  "Checks that neighbors are being combined properly."
  (let [test-seq [(forma-value nil 1 1 1) (forma-value nil 2 2 2)]]
    (combine-neighbors test-seq) => (neighbor-value (fire-value 0 0 0 0)
                                                    2
                                                    1.5 1
                                                    1.5 1
                                                    1.5 1)))

(fact "boundaries testing."
  (boundaries [ 0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]
  (boundaries [ 0 [1 2 3 4] [2 3 4 5]]) => (throws AssertionError))

(tabular
 (fact "adjust and adjust-timeseries testing, combined!"
   (adjust ?a0 ?a-vec ?b0 ?b-vec) => [?start ?a ?b]
   (adjust-timeseries
    (timeseries-value ?a0 ?a-vec)
    (timeseries-value ?b0 ?b-vec))
   =>
   [(timeseries-value ?start ?a)
    (timeseries-value ?start ?b)])
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
