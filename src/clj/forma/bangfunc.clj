(ns forma.bangfunc
  (:require [incanter.core :as i]
            [incanter.charts :as c])
  (:use clojure.contrib.def))

; test data to simulate a randome time-series (will be read in later)
(def random-ints (repeatedly #(rand-int 100)))
(def test-ts (take 131 random-ints))

(def test-ts [5 3 7 6 7 8 9 8 7 4 3 1 1 2 2 5 6 7 5 4 3 4 5 8 10 10 18 19 6 5 4 9 3 2 0 1 1 2 4 9 10 15 19 22 18 9 2 1 1 0])


; Useful general functions

(defn average
  "average of a list" 
  [lst] 
  (float (/ (reduce + lst) (count lst))))

(defn variance-matrix
  "construct a variance-covariance matrix from a given matrix X"
  [X]
  (i/solve (i/mmult (i/trans X) X)))

(defn ones-column
  "create a column of ones with length [x]"
  [x]
  (i/matrix (repeat x 1)))

;De-seasonalize time-series

(defn seasonal-rows [n]
  "lazy sequence of monthly dummy vectors"
  (vec
   (take n (partition 11 1 (cycle (cons 1 (repeat 11 0)))))))

(defn seasonal-matrix
  "create a matrix of [num-months]x11 of monthly dummies, where
  [num-months] indicates the number of months in the time-series"
  [num-months]
  (i/bind-columns (ones-column num-months)
                  (seasonal-rows num-months)))


(defn deseasonalize
  "deseasonalize a time-series [ts] using monthly dummies. returns
  a vector the same length of the original time-series, with desea-
  sonalized values."
  [ts]
  (let [avg-seq (repeat (count ts) (average ts))
        X    (seasonal-matrix (count ts))
        fix  (i/mmult (variance-matrix X) (i/trans X) ts)
        adj  (i/mmult (i/sel X :except-cols 0) (i/sel fix :except-rows 0))]
     (i/plus avg-seq (i/minus ts adj))))

(def plot1 (c/scatter-plot (i/matrix (range (count test-ts))) (deseasonalize test-ts)))
(add-lines plot1 (i/matrix (range (count test-ts))) test-ts)

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

;; WHIZBANG
;; need to get an associated time-series for rain. the function will
;; therefore change.

(defn with-rain-cofactor
  "construct a time-series with a ")

(defn std-error
  "get the standard error from a variance matrix")

(defn ols-coeff
  "get the trend coefficient from a time-series, given a variance matrix"
  (let [ycol (i/trans [ts])
        X (with-rain-cofactor (count ts))
        V (variance-matrix X)]))

(defn whiz-ols
  "extract both the OLS trend coefficient and the t-stat associated
   with the trend characteristic"
  [ts V]
  ((juxt ols-coeff whiz-ols)) ts V)



; (def whizbang ((juxt ols-coeff ols-std-error) test-ts))


; (defn make-matrix
;   [width height]
;   "Create a matrix (list of lists)"
;   (repeat width (repeat height 0)))
; 
; (defn matrix-map
;   [m func]
;   "Apply a function to every element in a matrix"
;   (map (fn [x] (map func x)) m))
