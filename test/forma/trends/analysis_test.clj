(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use [midje.sweet]
        [forma.matrix.utils]
        [forma.trends.stretch]
        [forma.trends.data]
        [forma.utils]
        [cascalog.api]
        [clojure.math.numeric-tower :only (sqrt floor abs expt)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]
            [forma.schema :as schema]))

(defn num-equals [expected]
  (fn [actual] (== expected actual)))

(fact
 "Checking outer product calculation against Numpy function np.outer() for mat and mat.T, where mat is [1 2 3]"
 (outer-product [1 2 3]) => [1.0 2.0 3.0 2.0 4.0 6.0 3.0 6.0 9.0])

(fact
 "Check element-wise sum of components of vector of vectors"
 (element-sum [[1 2 3] [1 2 3]]) => [2 4 6])

(fact
 "check raising residuals of linear model to given power"
 (let [y Yt
       X (idx Yt)
       power 2]
   (last (expt-residuals y X power))) => (roughly 0.044304))

(fact
  "Check that indexing is correct"
  (idx [4 5 6]) => [1 2 3])

(fact
  "Check windowed-map"
  (windowed-map ols-trend 2 [1 2 50]) => (contains (map roughly [1.0 48.0])))

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
 (fact
  "check scaling all elements of a vector by a scalar"
  (scale ?scalar ?coll) => ?expected)
 ?scalar ?coll ?expected
 1 [1 2 3] [1 2 3]
 2 [1 2 3] [2 4 6]
 1.5 [1 2 3] [1.5 3.0 4.5])

(facts
 "test that `long-stats` yields the trend coefficient and t-test
statistic on `ndvi`"
 (let [[coeff t-test] (long-stats ndvi)]
   coeff  => (roughly -1.14300)
   t-test => (roughly -0.91826)))

(fact
 "first-order-conditions has been checked"
 (map last (first-order-conditions ndvi))
 => [-440347.2867647055 -1624.8977371391347 89557.2243993124])

(fact
 "should return flattened (square) matrices of the element sums (read:
summing in place) of the first-order conditions and the cumulative
first-order conditions"
 (count (hansen-mats ndvi)) => 2)

(defn shift-down-end
  "returns a transformed collection, where the last half is shifted
  down by some factor"
  [coll]
  (let [half-count (floor (/ (count ndvi) 2))
        first-half (take half-count ndvi)
        second-half (drop half-count ndvi)]
    (concat first-half
            (map #(- % (/ % 2)) second-half))))

(fact
 "test that the Hansen test statistic is relatively high for a time
 series with a constructed, short-term break"
 (- (hansen-stat (shift-down-end ndvi))
    (hansen-stat ndvi)) => pos?)

(facts
 "check that the appropriate number of periods are included in the
 results vector, after the appropriate number of intervals (strictly
 within the training period) are dropped.  Suppose, for example, that
 there are exactly 100 intervals in the training period, with 271
 intervals total (length of test data, ndvi).  There should be 172
 values in the result vector: 1 to mark the end of the training
 period, and then 171 thereafter.

 Parameter list:

 30: length of long-block for OLS trend
 10: length of moving average window
 23: frequency of 16-day intervals (annually)
 100: example length of the training period
 271: last period in test time series"
 (count (telescoping-long-trend 23 100 271 ndvi reli rain)) => 172
 (count (first (telescoping-long-trend 23 100 271 ndvi reli rain))) => 3
 (count (telescoping-short-trend 30 10 23 100 ndvi reli))  => 172
 (last  (telescoping-short-trend 30 10 23 100 ndvi reli))  => (roughly -89.4561))

(fact
 "test that the magnitude of the short-term drop of the
 transformed (shifted down) time series is higher than that of the
 original time series"
 (let [s-drop (telescoping-short-trend 30 10 23 138 ndvi reli)
       big-drop (telescoping-short-trend 30 10 23 138 (shift-down-end ndvi) reli)]
   (- (abs (reduce min big-drop)) (abs (reduce min s-drop))) => pos?))

;; Benchmark

;; (time (dotimes [_ 1]
;;         (dorun (telescoping-long-trend 140 271 23 ndvi reli))))
;; "Elapsed time: 3320.463 msecs"

;; Newest implementation

;; (time (dotimes [_ 1]
;;         (dorun (collect-short-trend 30 10 23 100 ndvi reli))))
;; "Elapsed time: 52.322 msecs"

;; [for reference and encouragement] Original function, which mapped
;; the short-trend across small blocks

;; (time (dotimes [_ 1]
;;         (dorun (telescoping-short-trend 140 271 23 30 10 ndvi reli))))
;; "Elapsed time: 5650.48 msecs"


(def ts-tap
  (vec (repeat 2 {:start 0 :end 271 :ndvi ndvi :reli reli :rain rain})))

(defn long-trend-results
  "returns three vectors, the first with the hansen statistic for time
  series ranging from 0 through 135 (end of training period) and 136;
  the second with the long-term drop value; and the third with the
  long-term t-statisic.  Input a map, generated by total-tap.  The
  start and end index don't matter for this application, but are left
  in there anyway to ensure forward compatibility"
  [m]
  (let [ndvi (vector (:ndvi m))
        reli (vector (:reli m))
        rain (vector (:rain m))])
  (transpose (telescoping-long-trend 23 135 136 ndvi reli rain)))


(def long-trend-query
  (<- [?han-stat ?long-drop ?long-tstat]
      (ts-tap ?ts-map)
      (long-trend-results ?ts-map :> ?han-stat ?long-drop ?long-tstat)
      (:distinct false)))

(fact
 "duplicate time series will produce two sets of identical results, given that :distinct is set to false; otherwise, results would "
 long-trend-query => (produces '([[1.2393550741169639 1.2133709085855648]
                                  [2.4915869043482424 1.3049908881259853]
                                  [1.1504228201940752 0.5951173333726173]]
                                 [[1.2393550741169639 1.2133709085855648]
                                  [2.4915869043482424 1.3049908881259853]
                                  [1.1504228201940752 0.5951173333726173]])))

;; informative tests (leave in for now)

;; (defn create-tap
;;   [n ts]
;;   (vec (repeat n (schema/timeseries-value 0 ts))))

;; (defn myfunc [m] 
;;   (let [coll (vector (:series m))]
;;     (hansen-stat (first coll))))

;; (defn test-myfunc []
;;   (?<- (stdout) [?han] (time-src ?series) (myfunc ?series :> ?han)))

