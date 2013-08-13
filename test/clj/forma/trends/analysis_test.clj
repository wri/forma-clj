(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use [midje sweet]
        [forma.trends.data :only (ndvi rain reli)])
  (:require [incanter.core :as i]))

(facts "Test that `long-stats` yields the trend coefficient and t-test
 statistic on `ndvi`"

  ;; Long-term statistics on NDVI alone
  (let [[coeff t-test] (long-stats ndvi)]
    coeff  => (roughly -1.14300)
    t-test => (roughly -0.91826))

  ;; Long-term statistics on NDVI, with rain as a cofactor.  Note that
  ;; the trends change slightly.
  (let [[coeff t-test] (long-stats ndvi rain)]
    coeff  => (roughly -1.23824)
    t-test => (roughly -0.99763))

  ;; Long-term statistics on NDVI, with constant rain as a
  ;; cofactor. Given constant rain, it should be ignored and results
  ;; should be the same as for NDVI alone.
  (let [rain (repeat (count rain) 1)
        [coeff t-test] (long-stats ndvi rain)]
    coeff  => (roughly -1.14300)
    t-test => (roughly -0.91826))

  ;; Returns a pair of nil values in the presence of a singular matrix.
  (let [[coeff t-test] (long-stats [0])]
    coeff  => nil
    t-test => nil))

(fact "Test first-order conditions."
  (ffirst
   (first-order-conditions (i/matrix ndvi))) => (roughly -1094.50817))

(defn- shift-down-end
  "Returns a transformed collection, where the last half is shifted
  down by 50%; used to test that the hansen-stat identifies
  down-shifts in a time-series (see next test)."
  [coll]
  (let [half-count (Math/floor (/ (count ndvi) 2))
        second-half (drop half-count ndvi)]
    (concat (take half-count ndvi)
            (map #(- % (/ % 2)) second-half))))

(fact "Test that the Hansen test statistic is relatively high for a
  time series with a constructed, short-term break"
  (- (hansen-stat (i/matrix (shift-down-end ndvi)))
     (hansen-stat (i/matrix ndvi))) => pos?)

(facts
  "Test the value of the hansen stat, based off standard NDVI test
  series; test also that the hansen-stat will return nil for
  time-series that yield a singular first-order-condition matrix."
  (hansen-stat (i/matrix ndvi)) => (roughly 0.911317)
  (hansen-stat (repeat 100 0))  => nil)

(fact "Test `short-stat`."
  (short-stat 30 10 ndvi) => -63.334638487208096)

(facts
  "Test `short-stat-all`. Last value in output should be the same as
   the output for `short-stat` (which uses `(reduce min stats)` to
   select output). `short-stat-all` on the other hand uses
   `(reductions min stats)`, so the result should contain
   `(- (count ndvi) 29 9)` => 233 values given the input for these tests."
  (last (short-stat-all 30 10 ndvi)) => -63.334638487208096
  (count   (short-stat-all 30 10 ndvi)) => 233)
