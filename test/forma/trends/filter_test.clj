;; This namespace tests the timeseries filtering, which includes
;; functions that support deseasonalization, timeseries smoothing, and
;; the adjustment of unreliable values in a timeseries.

(ns forma.trends.filter-test
  (:use [forma.trends.filter] :reload)
  (:use [forma.trends.data]
        [midje.sweet]
        [clojure.math.numeric-tower :only (abs)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]))

(fact "Check that the dimension of the cofactor matrix corresponds to
46 time periods with 23 'seasons'."
  (i/dim (dummy-mat 23 (count ndvi))) => [271 23])

(fact "The average of the original timeseries should equal the average
of the deseasonalized timeseries (perhaps subject to rounding error)."
  (let [decomp (deseasonalize 23 ndvi)]
    (count decomp)  => (count ndvi)
    (s/mean decomp) => (roughly (s/mean ndvi))
    (last decomp)   => (roughly 6265.03629)))

(fact "Check that harmonic seasonal decomposition yields a vector with the
same length as the original vector, with the same mean."
  (let [decomp (harmonic-seasonal-decomposition 23 3 ndvi)]
    (count decomp)  => (count ndvi)
    (s/mean decomp) => (roughly (s/mean ndvi))
    (last decomp)   => (roughly 6182.56822)))

(fact "Check that the HP-filter yields a vector with the same length
as the original vector, with the same mean."
  (let [decomp (hp-filter 100 ndvi)]
    (count decomp)  => (count ndvi)
    (s/mean decomp) => (roughly (s/mean ndvi))
    (last decomp)   => (roughly 6717.28322)))

(facts "Show the `bad-ends` function in action, mostly as an
illustration."
  (let [bad-set #{2 3 255}
        return-bad (fn [coll] (bad-ends bad-set coll))]    
    (return-bad [0 0 0 0 0]) => #{}
    (return-bad [1 0 0 0 1]) => #{}
    (return-bad [2 0 0 0 0]) => #{0}
    (return-bad [2 0 0 0 2]) => #{0 4}
    (return-bad [2 2 0 2 2]) => #{0 1 3 4}
    (return-bad [2 2 2 2 2]) => #{0 1 2 3 4}))

(tabular
 (fact "Neutralize bad values at the start or beginning of a
    timeseries, replacing them with mean of good values"
   (neutralize-ends #{2 3} ?reli-coll [0 1 2 3 4]) => ?result)
 ?reli-coll ?result
 [0 0 0 0 0] [0 1 2 3 4]
 [0 2 0 0 0] [0 1 2 3 4]
 [2 0 0 0 0] [2.5 1 2 3 4]
 [2 0 0 0 2] [2.0 1 2 3 2.0]
 [2 2 0 0 2] [2.5 2.5 2 3 2.5])

(fact "Check that stretching starts at left-idx and ends before
right-idx"
  (let [left-idx 1
        right-idx 4]
    (stretch-ts [1 2 -3000 -3000 5] [left-idx right-idx]) => [2.0 3.0 4.0]))

(fact "Check that mask is appropriately created"
  (let [pred (partial > 0)]
    (mask pred [1 -1 1] [2 2 2]) => [2 nil 2]))

(fact "replace values of original column with the `replace-val` at the
specified indices."
  (let [idx-set #{2 4} replace-val 50]
    (replace-index-set idx-set replace-val [0 0 0 0 0 0])
    => [0 0 50 0 50 0]))

(facts "Check that filtering cloudy or otherwise poor quality
observations does not change the length of the original vector."
 (let [bad-set #{2 3 255}
       good-set #{1 0}]
   (count (neutralize-ends bad-set ndvi reli)) => (count ndvi)
   (count (make-reliable bad-set good-set ndvi reli)) => (count ndvi)))

(fact "number of values that are different in ndvi-test, after
applying the final compositions of functions."
  (let [reliable (make-reliable #{3 2} #{1 0} ndvi reli)]
    (count (filter (complement zero?)
                   (map - reliable ndvi)))) => 175)

(tabular
 (fact "check that `make-reliable` works for dummy, small examples; a
 sort of stress test for `make-reliable`."
   (make-reliable #{0 1} #{2 3 255} ?spectral-ts ?reli-ts) => ?res)
 ?spectral-ts ?reli-ts ?res
 [1 1 1] [0 0 0] [1 1 1]
 [1 1 1] [0 1 0] [1 1 1]
 [1 1 1] [1 1 1] [1 1 1]
 [1 1 1] [2 2 1] [1.0 1.0 1]
 [1 1 1] [2 2 2] nil
 [1 2 3] [1 2 1] [1.0 2.0 3])

(fact "show that the telescoping timeseries function has the proper
range for a small, dummy example; specifically that the first element
of the result has three elements and the last element of the result
has five elements."
  (tele-ts 3 5 [0 1 2 3 4 5 6 7])
  => [[0 1 2] [0 1 2 3] [0 1 2 3 4]])

(fact "ensure that the beginning of the fully clean -- reliable and
deseasonalized -- NDVI timeseries matches true values. "
  (make-clean 23 #{0 1} #{2 3 255} ndvi reli)
  => (has-prefix [7753 7955 7635 8360 7888]))

(fact "Make sure rain is shortened to length of input ts"
  (shorten-ts [1 2 3] [1 2 3 4 5]) => [[1 2 3]])
