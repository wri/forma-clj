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
 (count (neutralize-ends #{3 2} reli ndvi)) => (count ndvi)
 (count (make-reliable #{3 2} #{1 0} reli ndvi)) => (count ndvi))

(fact
 "number of values that are different in ndvi-test, after applying the
final compositions of functions."
 (let [reliable (make-reliable #{3 2} #{1 0} reli ndvi)]
   (count
    (filter (complement zero?)
            (map - reliable ndvi)))) => 96)

