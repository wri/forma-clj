(ns forma.trends.filter-test
  (:use [forma.utils :only (average)]
        [forma.trends.filter]
        [forma.trends.data]
        [midje.sweet]
        [clojure.math.numeric-tower :only (abs)])
  (:require [incanter.core :as i]))

(fact
  (dummy-mat 2 2) => (i/matrix [[1 0] [0 1]]))

(fact
  (deseasonalize 3 [1 2 3 4 3 2 1 2 3 4 3 2 1]) => [1.1846153846153844 1.8846153846153846 2.8846153846153846 4.184615384615384 2.8846153846153846 1.8846153846153846 1.1846153846153844 1.8846153846153846 2.8846153846153846 4.184615384615384 2.8846153846153846 1.8846153846153846 1.1846153846153844])

(facts
 "harmonic series should have the same length as input collection, but
with two columns; the first is a scaled vector, passed through cosine,
which implies all values less than or equal to 1; the `23` parameter
indicates the number of 16-day intervals in a year."
 (let [harmony (harmonic-series 23 ndvi 3)
       cos-harmony (first harmony)]
   (count harmony) => 2
   (count cos-harmony) => (count ndvi)
   (count (filter #(> % 1) (map abs cos-harmony))) => 0))

(future-fact
 "k-harmonic-matrix produces correct results"
  (k-harmonic-matrix 23 3 [10 10 10 10 10 ]))

(fact
 "check that the decomposition is the same length with roughly the
same mean as the original time series"
 (let [decomp (harmonic-seasonal-decomposition 23 3 ndvi)]
   (count decomp) => (count ndvi)
   (float (average decomp)) => (roughly (average ndvi))))

(future-fact
 "hp-mat produces correct results")

(future-fact
 "hp-filter produces correct results")

(facts
  "`interpolate` is actually linear interpolation"
  (interpolate 2 4 3) => [(float 2.0) (float 2.6666667) (float 3.3333333)]
  (interpolate 2 4 2) => [(float 2.0) (float 3.0)])

(fact
  "Check that stretching starts at left-idx and ends before right-idx"
  (let [left-idx 1
        right-idx 4]
    (stretch-ts [1 2 -3000 -3000 5] [left-idx right-idx]) => [2.0 3.0 4.0]))

(fact
  "Check that mask is appropriately created"
  (let [pred (partial > 0)]
    (mask pred [1 -1 1] [2 2 2]) => [2 nil 2]))

(fact
  (let [idx-set #{2 4}
        replace-val 50]
    (replace-index-set idx-set replace-val [0 0 0 0 0 0]) => [0 0 50 0 50 0]))

(tabular
 (fact
   (let [bad-set #{2 3 255}]
     (bad-ends bad-set ?coll) => ?result))
 ?coll ?result
 [0 0 0 0 0] #{}
 [1 0 0 0 1] #{}
 [2 0 0 0 0] #{0}
 [2 0 0 0 2] #{0 4}
 [2 2 0 2 2] #{0 1 3 4}
 [2 2 2 2 2] #{0 1 2 3 4})

(tabular
 (fact
   "Test that `apply-to-valid` applies `func` only to valid values in `coll`"
   (let [func (partial reduce +)]
     (apply-to-valid func ?coll) => ?result))
 ?coll ?result
 [1 2 3 4] 10
 [1 2 nil 4] 7)

(tabular
 (fact
   "Neutralize bad values at the start or beginning of a timeseries, replacing them
    with mean of good values"
   (neutralize-ends #{2 3} ?reli-coll [0 1 2 3 4]) => ?result)
 ?reli-coll ?result
 [0 0 0 0 0] [0 1 2 3 4]
 [0 2 0 0 0] [0 1 2 3 4]
 [2 0 0 0 0] [2.5 1 2 3 4]
 [2 0 0 0 2] [2.0 1 2 3 2.0]
 [2 2 0 0 2] [2.5 2.5 2 3 2.5])

(facts
 "that filtering cloudy or otherwise poor quality observations does not change the length of the vector"
 (count (neutralize-ends #{3 2 255} ndvi reli)) => (count ndvi)
 (count (make-reliable #{3 2 255} #{1 0} ndvi reli)) => (count ndvi))

(fact
 "number of values that are different in ndvi-test, after applying the
final compositions of functions."
 (let [reliable (make-reliable #{3 2} #{1 0} ndvi reli)]
   (count (filter (complement zero?)
                  (map - reliable ndvi)))) => 175)

(tabular
 (fact
   (make-reliable #{0 1} #{2 3 255} ?spectral-ts ?reli-ts) => ?res)
 ?spectral-ts ?reli-ts ?res
 [1 1 1] [0 0 0] [1 1 1]
 [1 1 1] [0 1 0] [1 1 1]
 [1 1 1] [1 1 1] [1 1 1]
 [1 1 1] [2 2 1] [1.0 1.0 1]
 [1 1 1] [2 2 2] nil
 [1 2 3] [1 2 1] [1.0 2.0 3]
 (vec (repeat 10 10)) (vec (repeat 10 3)) nil)

(fact
  "Checks `tele-ts` to ensure that indexing is correct. Plenty of room
   for confusion and one-off errors if you forget that under the hood
   this is calling `subvec` and using the provided start/end indices
   as the final index with `subvec`. The other value is always 0."
  (tele-ts 3 5 [0 1 2 3 4 5 6 7]) => [[0 1 2] [0 1 2 3] [0 1 2 3 4]])

(fact
  (make-clean 23 #{0 1} #{2 3 255} ndvi reli) => (has-prefix [7753 7955 7635 8360 7888]))

(fact
  "Make sure rain is shortened to length of input ts"
  (shorten-ts [1 2 3] [1 2 3 4 5]) => [[1 2 3]])

