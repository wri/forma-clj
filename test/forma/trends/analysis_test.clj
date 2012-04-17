(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use [cascalog.api]
        [forma.matrix.utils :only (transpose)]
        [midje sweet cascalog]
        [forma.trends.data :only (ndvi rain reli Yt)]
        [forma.utils :only (idx)]
        [forma.schema :only (timeseries-value)]
        [forma.trends.stretch :only (ts-expander)]
        [clojure.math.numeric-tower :only (floor abs expt)]
        [clojure.test :only (deftest)])
  (:require [incanter.core :as i]))

(facts
 "test that `long-stats` yields the trend coefficient and t-test
statistic on `ndvi`"
 (let [[coeff t-test] (long-stats ndvi)]
   coeff  => (roughly -1.14300)
   t-test => (roughly -0.91826)))

(fact
 "first-order-conditions has been checked"
 (last (map last (first-order-conditions (i/matrix ndvi)))) => (roughly 89557.2243))

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
 (- (hansen-stat (i/matrix (shift-down-end ndvi)))
    (hansen-stat (i/matrix ndvi))) => pos?)

(fact
  "test the value of the hansen stat, based off standard NDVI test series"
  (hansen-stat (i/matrix ndvi)) => (roughly 0.911317))

(def ts-tap
  "sample tap that mimics 2 identical pixels (each with the same time series)"
  (vec (repeat 2 {:start 0 :end 271 :ndvi ndvi :reli reli :rain rain})))

