(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use forma.schema
        [midje sweet cascalog])
  (:require [forma.date-time :as date])
  (:import [backtype.hadoop.pail Pail]
           [forma.schema ModisChunkLocation]
           [forma.hadoop.pail SplitDataChunkPailStructure]))

;; TODO: We need to move the tests for the schema into `forma.schema-test`.

(tabular
 (fact "Globstring test."
   (apply globstring ?args) => ?result)
 ?args                        ?result
 [* "hi" ["008006" "033011"]] "*/hi/{008006,033011}")


;; ## Various schema tests

(def neighbors
  [(forma-value nil 1 1 1)
   (forma-value (fire-value 1 1 1 1) 2 2 2)])

(fact
  "Tests that the combine neighbors function produces the proper
textual representation."
  (let [s "0\t0\t0\t0\t1.0\t1.0\t1.0\t1\t1\t1\t1\t2\t1.5\t1.0\t1.5\t1.0\t1.5\t1.0"]
    (textify (first neighbors)
             (combine-neighbors neighbors)) => s))

(fact
  "Checks that neighbors are being combined properly."
  (let [test-seq [(forma-value nil 1 1 1) (forma-value nil 2 2 2 )]]
    (combine-neighbors test-seq) => (neighbor-value (fire-value 0 0 0 0)
                                                    2
                                                    1.5 1.0
                                                    1.5 1.0
                                                    1.5 1.0)))

(fact "struct-edges tests."
  (struct-edges [ 0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]
  (struct-edges [ 0 [1 2 3 4] [2 3 4 5]]) => (throws AssertionError))

(fact "trim-seq tests."
  (trim-seq 0 2 1 [1 2 3]) => [1]
  (trim-seq 0 3 0 [1 2 3]) => [1 2 3]
  (trim-seq 5 10 0 [1 2 3]) => nil)

(tabular
 (fact "adjust and adjust-timeseries testing, combined!"
   (let [[av bv a b] (map to-struct [?a-vec ?b-vec ?a ?b])]
     (adjust ?a0 av ?b0 bv) => [?start a b]
     (adjust-timeseries
      (timeseries-value ?a0 (to-struct ?a-vec))
      (timeseries-value ?b0 (to-struct ?b-vec)))
     =>
     [(timeseries-value ?start (arrayize ?a))
      (timeseries-value ?start (arrayize ?b))]))
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
 0       (timseries-value  f-start [(fire-value 0 0 0 1) (fire-value 1 1 1 1)])
 1       (timeseries-value f-start [(fire-value 0 0 0 1)])
 2       nil)

(fact "chunk to pixel location conversions."
  (-> (ModisChunkLocation. "1000" 10 10 59 24000)
      (chunkloc->pixloc 23999)) => (pixel-location "1000" 10 10 1199 1199))

;; TODO: Integrate these into some tests.
(def some-pail
  (let [path "/tmp/pail"]
    (try (Pail. path)
         (catch Exception e
           (Pail/create path (SplitDataChunkPailStructure.))))))

;; TODO: Consolidate these two.
(defn gen-tuples [dataset m-res t-res]
  (->> (for [data (range 1000)]
         (chunk-value dataset t-res "2005-12-01" m-res 8 6 x 1 data))
       (into [])))

(defn tuple-writer [dataset m-res t-res]
  (with-open [stream (.openWrite some-pail)]
    (doseq [data (range 1000)]
      (.writeObject stream
                    (chunk-value dataset t-res "2005-12-01" m-res 8 6 x 1 data)))
    (.consolidate some-pail)))

(future-fact "tuple-writing tests!")

;; (?pail- (split-chunk-tap "/tmp/output")
;;         (<- [?a] ((tuple-src "precl" "1000" "32") ?a)))
