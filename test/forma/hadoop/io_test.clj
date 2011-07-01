(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use midje.sweet)
  (:require [forma.date-time :as date]))

(tabular
 (fact "Globstring test."
   (apply globstring "s3n://bucket/" ?args) => (str "s3n://bucket/" ?result))
 ?args                        ?result
 [* "hi" ["008006" "033011"]] "*/hi/{008006,033011}")


;; ## Various schema tests

(def neighbors [(forma-value nil 1 1 1)
                (forma-value nil 2 2 2)])

(fact
  "Tests that the combine neighbors function produces the proper
textual representation."
  (let [s "1 1 1 1 0 0 0 0 1.0 1.0 1.0 0 0 0 0 2 1.5 1.0 1.5 1.0 1.5 1.0"]
    (textify 1 1 1 1
             (first neighbors)
             (combine-neighbors neighbors)) => s))

(fact
  "Checks that neighbors are being combined properly."
  (let [test-seq [(forma-value nil 1 1 1) (forma-value nil 2 2 2 )]]
    (combine-neighbors test-seq) => (neighbor-value (fire-tuple 0 0 0 0)
                                                    2
                                                    1.5 1.0
                                                    1.5 1.0
                                                    1.5 1.0)))

(tabular
 (fact "count-vals test."
   (count-vals ?thriftable) => ?n
   ?n ?thriftable
   2  [1 2]
   3  (int-struct [1 3 2])
   1  (int-struct [1])
   3  (int-struct [5.4 32 12.0])))

(fact "struct-edges tests."
  (struct-edges [ 0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]
  (struct-edges [ 0 [1 2 3 4] [2 3 4 5]]) => (throws AssertionError))

(fact "trim-struct tests."
  (trim-struct 0 2 1 [1 2 3]) => (to-struct [1])
  (trim-struct 0 3 0 [1 2 3]) => (to-struct [1 2 3])
  (trim-struct 5 10 0 [1 2 3]) => nil)

(tabular
 (fact "adjust testing."
   (let [[av bv a b] (map to-struct [?a-vec ?b-vec ?a ?b])]
     (adjust ?a0 av ?b0 bv) => [?start a b]))
 ?a0 ?a-vec    ?b0 ?b-vec    ?start ?a      ?b
 0   [1 2 3 4] 1   [2 3 4 5] 1      [2 3 4] [2 3 4]
 2   [9 8 7]   0   [1 2 3 4] 2      [9 8]   [3 4]
 10  [2 3 4]   1   [1 2 3]   10     []      [])

(facts "adjust-fires testing."
  (let [est-map {:est-start "2005-01-01"
                 :est-end "2005-02-01"
                 :t-res "32"}
        f-period (date/datetime->period "32" "2005-01-01")
        f-series (fire-series [(fire-tuple 0 0 0 1)
                               (fire-tuple 1 1 1 1)])]
    (adjust-fires est-map f-period f-series) => [f-period f-series]
    (adjust-fires est-map (inc f-period) f-series) => [f-period
                                                       (fire-series [(fire-tuple 0 0 0 1)])]
    (adjust-fires est-map (+ 2 f-period) f-series) => [420 nil]))
