(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use [midje.sweet]
        [forma.trends.data]
        [forma.matrix.utils]
        [clojure.math.numeric-tower :only (sqrt floor abs expt)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]))

(defn num-equals [expected]
  (fn [actual] (== expected actual)))

(fact
  "Check square matrix"
  (let [mat (i/matrix [[1 2 3 4]
                       [5 6 7 8]
                       [9 10 11 12]
                       [13 14 15 16]])]
    (is-square? mat) => true))

(fact
  "check singular matrix"
  (let [mat (i/matrix [[3 6]
                       [1 2]])]
    (singular? mat) => true))

(fact
  "Check that indexing is correct"
  (idx [4 5 6]) => [1 2 3])

(fact
  "Check windowed-map
  Split test in two because of roughly limitations"
  (first (windowed-map ols-trend 2 [1 2 50])) => (roughly (first [1. 48.])))
(fact
  (last (windowed-map ols-trend 2 [1 2 50])) => (roughly (last [1. 48.])))

(fact
  "Does transpose work as planned? Sure looks like it!"
  (transpose [[1 2 3] [4 5 6]] ) => [[1 4] [2 5] [3 6]])

(fact
  "Checking outer product calculation against Numpy function np.outer() for mat and mat.T, where mat is [1 2 3]"
  (outer-product [1 2 3]) => [1.0 2.0 3.0 2.0 4.0 6.0 3.0 6.0 9.0])

(fact
  "Check element-wise sum of components of vector of vectors"
  (element-sum [[1 2 3] [1 2 3]]) => [2 4 6])

(fact
  "Check average of vector.
   Casting as float to generalize for another vector as necessary"
  (float (average [1 2 3.])) => (num-equals 2.0))

(fact
  "Check moving average"
  (moving-average 2 [1 2 3]) => [3/2 5/2])

(tabular
 (fact
   "Calculates simple OLS trend, assuming 0 intercept."
   (ols-trend ?v) => ?expected)
 ?v ?expected
 [1 2] (roughly 1. 0.00000001)
 [1 2 4] (roughly 1.5 0.00000001)
 [1 2 50] (roughly 24.5 0.00000001)
 [2 50] (roughly 48. 0.00000001))

(tabular
 "check calculation of minimum short-term trend. ?long is the window, ?short is the moving average smoothing"
 (fact
   (min-short-trend ?long ?short ?ts) => ?expected)
 ?long ?short ?ts ?expected
 2      1    [1 2 3 4 3 2 1] (roughly -1.)
 3      1    [1 2 3 4 3 2 1] (roughly -1.)
 3      1    [1 2 3 4 3 2 0] (roughly -1.5)
 3      2    [1 2 3 4 3 2 0] (roughly -1.25))

(fact
  "check raising residuals of linear model to given power"
  (let [y Yt
        X (idx Yt)
        power 2]
    (last (expt-residuals y X power))) => 0.04430432657988476)

(tabular
 (fact
   "check scaling all elements of a vector by a scalar"
   (scaled-vector ?scalar ?coll) => ?expected)
 ?scalar ?coll ?expected
 1 [1 2 3] [1 2 3]
 2 [1 2 3] [2 4 6]
 1.5 [1 2 3] [1.5 3.0 4.5])

(facts
 "test that `long-stats` yields the trend coefficient and t-test
statistic on `ndvi`"
 (let [[coeff t-test] (long-stats ndvi)]
   coeff  => -1.1430015917806315
   t-test => -0.918260660209))

(fact
 "first-order-conditions has been checked"
 (map last (first-order-conditions ndvi))
 => [-440347.2867647055 -1624.8977371391347 89557.2243993124])

(fact
 "should return flattened (square) matrices of the element sums (read:
summing in place) of the first-order conditions and the cumulative
first-order conditions"
 (count (hansen-mats ndvi)) => 2)

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

