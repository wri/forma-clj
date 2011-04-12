(ns forma.bangfunc
  (:require [incanter.core :as i]))

; test data to simulate a randome time-series (will be read in later)
(def random-ints (repeatedly #(rand-int 100)))
(def test-ts (take 131 random-ints))

(def test-ts [5 3 7 6 7 8 9 8 7 4 3 1 1 2 2 5 6 7 5 4 3 4 5 8 10 10 18 19 6 5 4 9 3 2 0 1 1 2 4 9 10 15 19 22 18 9 2 1 1 0])

(defn average [lst] (float (/ (reduce + lst) (count lst))))


; De-seasonalize time-series
; 
; mu = np.average(Y1)
; fix = np.linalg.inv(Xdt * Xd) * Xdt * Y1                                #Regress original timeseries on 11 monthly dummies and ones-vector (for constant)
; Y1 = ((Y1 - Xd[:,1:12] * fix[1:12]) - fix[0]) + mu                      #Adjust the NDVI values by monthly averages


(defn variance-matrix
  "construct a variance-covariance matrix from a given matrix X"
  [X]
  (i/solve (i/mmult (i/trans X) X)))

(defn make-matrix
  [width height]
  "Create a matrix (list of lists)"
  (repeat width (repeat height 0)))

(defn matrix-map
  [m func]
  "Apply a function to every element in a matrix"
  (map (fn [x] (map func x)) m))

(def seasonal-vector (vec (conj (repeat 11 0) 1)))

(defn seasonal-matrix
  "create a matrix of [num-months]x11 of monthly dummies, where
  [num-months] indicates the number of months in the time-series"
  [num-months]
  )

(def monthly-dummies 
  (i/bind-rows (repeat 11 0) (i/identity-matrix 11)))

(defn deseasonalize
  "deseasonalize a time-series [ts] using monthly dummies."
  [ts]
  (let [avg (average ts)]
  avg))

; WHOOPBANG

(defn whoop-cofactor
  "construct a matrix of cofactors; first column is comprised 
  of ones, second column is a range from 1 to [num-months].  The 
  result is a [num-months]x2 incanter matrix."
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
  (->> ts
       (windowed-apply whoop-ols long-block)
       (windowed-apply average window)
       (reduce min)))

; (def whizbang (ols-coeff test-ts))
; juxt function ((juxt a b) x) -> ((a x) (b x))