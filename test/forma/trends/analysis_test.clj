(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use [midje sweet]
        [forma.trends.data :only (ndvi rain reli)])
  (:require [incanter.core :as i]))

(facts
 "test that `long-stats` yields the trend coefficient and t-test
statistic on `ndvi`"
 (let [[coeff t-test] (long-stats ndvi)]
   coeff  => (roughly -1.14300)
   t-test => (roughly -0.91826)))

(fact
 "first-order-conditions has been checked"
 (last (map last (first-order-conditions (i/matrix ndvi))))
 => (roughly 89557.2243))

(defn- shift-down-end
  "Returns a transformed collection, where the last half is shifted
  down by some factor; used to test that the hansen-stat identifies
  down-shifts in a time-series (see next test)."  [coll]
  (let [half-count (Math/floor (/ (count ndvi) 2))
        first-half (take half-count ndvi)
        second-half (drop half-count ndvi)]
    (concat first-half
            (map #(- % (/ % 2)) second-half))))

(fact
 "test that the Hansen test statistic is relatively high for a time
 series with a constructed, short-term break"
 (- (hansen-stat (i/matrix (shift-down-end ndvi)))
    (hansen-stat (i/matrix ndvi))) => pos?)

(facts
  "test the value of the hansen stat, based off standard NDVI test
series; test also that the hansen-stat will return nil for time-series
that yield a singular first-order-condition matrix."
  (hansen-stat (i/matrix ndvi)) => (roughly 0.911317)
  (hansen-stat (repeat 100 0))  => nil)

(def ts-tap
  "sample tap that mimics 2 identical pixels (each with the same time
series)"
  (vec (repeat 2 {:start 0 :end 271 :ndvi ndvi :reli reli :rain rain})))

