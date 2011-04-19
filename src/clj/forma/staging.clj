(ns forma.staging
  (:use [forma.matrix.utils :only (variance-matrix)]
        [forma.trends.filter :only (hp-filter fix-time-series)]
   [clojure.contrib.seq :only (positions)]
   [clojure.contrib.math :only (round)])
  (:require [incanter.core :as i]
            [incanter.charts :as c]
            [clj-time.core :as time]))



(defn devel-namespaces []
  (use '(incanter core charts)))



; test data to simulate a randome time-series (will be read in later)
(def random-ints (repeatedly #(rand-int 100)))
(def test-ts (take 131 random-ints))



;; (knocked-off #(< % 0) tvec rvec)
;; don't need this right now for the filter
(defn knocked-off
  "shorten time-series on both ends to ensure that first and last
  values are good, according to the pred, quality-coll"
  [pred value-coll quality-coll]
  (let [bad-count (first (positions pred quality-coll))]
    (drop bad-count value-coll)))


;; WHIZBANG
;; need to get an associated time-series for rain. the function will
;; therefore change.


