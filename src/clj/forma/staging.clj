(ns forma.staging
  (:use [forma.matrix.utils :only (replace-val variance-matrix)]
        [forma.trends.filter :only (hp-filter)]
   [clojure.contrib.seq :only (positions)]
   [clojure.contrib.math :only (round)])
  (:require [incanter.core :as i]
            [incanter.charts :as c]
            [clj-time.core :as time]))

(defn devel-namespaces []
  (use '(incanter core charts)))

(def ndvi [7417 7568 7930 8049 8039 8533 8260 8192 7968 7148 7724 8800 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538 7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698 7838 8967 8136 7532 7838 8009 8136 8400 8219 8051 8091 7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813 8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216 7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195 8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041 7250 7884 8105 8033 8340 8288 7691 7599 8480 8563 8033 7708 7575 7996 7739 8058 7400 6682 7999 7655 7533 7904 8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878 8524 8167 7526 7330 7325 7485 8108 7978 7035 7650])

; test data to simulate a randome time-series (will be read in later)
(def random-ints (repeatedly #(rand-int 100)))
(def test-ts (take 131 random-ints))

(def kill-vals (replace-val ndvi > 7000 nil))

;; (knocked-off #(< % 0) tvec rvec)
;; don't need this right now for the filter
(defn knocked-off
  "shorten time-series on both ends to ensure that first and last
  values are good, according to the pred, quality-coll"
  [pred value-coll quality-coll]
  (let [bad-count (first (positions pred quality-coll))]
    (drop bad-count value-coll)))

(def reli (replace-val (replace-val ndvi < 7000 2) > 2 1))

;; WHIZBANG
;; need to get an associated time-series for rain. the function will
;; therefore change.

(defn with-rain-cofactor
  "construct a time-series with a "
  [x]
  x)

(defn std-error
  "get the standard error from a variance matrix"
  [x]
  (+ x 2))

(defn ols-coeff
  "get the trend coefficient from a time-series, given a variance matrix"
  [ts]
  (let [ycol (i/trans [ts])
        X (with-rain-cofactor (count ts))
        V (variance-matrix X)]))

(defn whiz-ols
  "extract both the OLS trend coefficient and the t-stat associated
   with the trend characteristic"
  [ts V]
  ((juxt ols-coeff whiz-ols)) ts V)


;; range between two dates in months



(defn num-years-to-milliseconds [x] (* 365 24 60 60 1000 x))
(def offset (num-years-to-milliseconds (- 2000 1970)))
(def dates
  (map (comp #(+ offset %)
             num-years-to-milliseconds
             #(/ % 12))
       (range (count ndvi))))


(def plot1 (c/time-series-plot dates
                               (hp-filter (fix-time-series #{1} reli ndvi) 2)
                               :title "NDVI and The H-P Filter"
                               :x-label "Year"
                               :y-label "NDVI values"))
;; (c/add-lines plot1 dates ndvi)
(c/add-lines plot1 dates (fix-time-series #{1} reli-test ndvi))
;; (c/add-points plot1 dates kill-vals)

(let [x dates]
  (c/slider #(i/set-data plot1
                         [x (hp-filter (fix-time-series #{1}
                                                        reli
                                                        ndvi)
                                       %)])
            (range 0 11 0.1)
            "lambda"))

