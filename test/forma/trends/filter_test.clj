(ns forma.trends.filter-test
  (:use [forma.utils :only (average)]
        [forma.trends.filter]
        [forma.trends.data]
        [midje.sweet]
        [clojure.math.numeric-tower :only (abs)]))

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

(fact
 "check that the decomposition is the same length with roughly the
same mean as the original time series"
 (let [decomp (harmonic-seasonal-decomposition 23 3 ndvi)]
   (count decomp) => (count ndvi)
   (float (average decomp)) => (roughly (average ndvi))))

(facts
 "that filtering cloudy or otherwise poor quality observations does not change the length of the vector"
 (count (neutralize-ends #{3 2} ndvi reli)) => (count ndvi)
 (count (make-reliable #{3 2} #{1 0} ndvi reli)) => (count ndvi))

(facts
  "`interpolate` is actually linear interpolation"
  (interpolate 2 4 3) => [(float 2.0) (float 2.6666667) (float 3.3333333)]
  (interpolate 2 4 2) => [(float 2.0) (float 3.0)])

(fact
 "number of values that are different in ndvi-test, after applying the
final compositions of functions."
 (let [reliable (make-reliable #{3 2} #{1 0} ndvi reli)]
   (count (filter (complement zero?)
                  (map - reliable ndvi)))) => 175)

(tabular
 (fact
   (make-clean 1 #{0 1} #{2 3 255} ?spectral-ts ?reli-ts) => ?res)
 ?spectral-ts ?reli-ts ?res
 [1 1 1] [0 0 0] [1 1 1]
 [1 1 1] [0 1 0] [1 1 1]
 [1 1 1] [1 1 1] [1 1 1]
 [1 1 1] [2 2 1] [1.0 1.0 1]
 [1 1 1] [2 2 2] nil
 [1 2 3] [1 2 1] [1.0 2.0 3]
 (vec (repeat 10 10)) (vec (repeat 10 3)) nil)