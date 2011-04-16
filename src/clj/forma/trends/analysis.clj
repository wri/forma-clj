(ns forma.trends.analysis
  (:use [forma.matrix.utils :only (variance-matrix average)]
        [forma.trends.filter :only (deseasonalize)])
  (:require [incanter.core :as i]))

; WHOOPBANG
 
(defn whoop-cofactor
  "construct a matrix of cofactors; first column is comprised of ones,
  second column is a range from 1 to [num-months].  The result is a
  [num-months]x2 incanter matrix."
  [num-months]
  (i/trans (i/bind-rows (repeat num-months 1) (range 1 (inc num-months)))))

(defn whoop-ols
  "extract OLS coefficient from a time-series as efficiently 
  as possible for the whoopbang function."
  [ts]
  (let [ycol (i/trans [ts])
        X (whoop-cofactor (count ts))
        V (variance-matrix X)]
  (i/sel (i/mmult V (i/trans X) ycol) 1 0)))

(defn windowed-apply 
  "apply a function [f] to a window along a sequence [xs] of length [window]"
  [f window xs]
  (map f (partition window 1 xs)))

(defn whoopbang
  "whoopbang will find the greatest OLS drop over a timeseries [ts] given 
  sub-timeseries of length [long-block].  The drops are smoothed by a moving 
  average window of length [mov-avg]."
  [ts long-block window]
  (->> (deseasonalize ts)
       (windowed-apply whoop-ols long-block)
       (windowed-apply average window)
       (reduce min)))

