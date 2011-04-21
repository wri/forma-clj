(ns forma.trends.filter
  (:use [forma.matrix.utils :only (ones-column
                                   average
                                   variance-matrix)]

        [clojure.contrib.seq :only (positions)])
  (:require [incanter.core :as i]))

;; De-seasonalize time-series

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
    (i/minus ts adj)))

;; Hodrick-Prescott filter

(defn insert-at
  "insert list [b] into list [a] at index [idx]."
  [idx a b]
  (let [opened (split-at idx a)]
    (concat (first opened) b (second opened))))

(defn insert-into-zeros
  "insert vector [v] into a vector of zeros of total length [len]
  at index [idx]."
  [idx len v]
  (insert-at idx (repeat (- len (count v)) 0) v))

(defn hp-mat
  "create the matrix of coefficients from the minimization problem
  required to parse the trend component from a time-series of le-
  gth [T], which has to be greater than or equal to 9 periods."
  [T]
  {:pre [(>= T 9)]
   :post [(= [T T] (i/dim %))]}
  (let [[first second :as but-2]
        (for [x (range (- T 2))
              :let [idx (if (>= x 2) (- x 2) 0)]]
          (insert-into-zeros idx T (cond (= x 0)  [1 2 -1]
                                         (= x 1)  [-2 5 4 1]
                                         :else [1 -4 6 -4 1])))]
    (i/matrix (concat but-2 (map reverse [second first])))))

(defn hp-filter
  "return a smoothed time-series, given the HP filter parameter."
  [ts lambda]
  (let [T (count ts)
        coeff-matrix (i/mult lambda (hp-mat T))
        trend-cond (i/solve (i/plus coeff-matrix (i/identity-matrix T)))]
    (i/mmult trend-cond ts)))

(defn interpolate
  [x y length]
  (let [delta (/ (- y x) (dec length))]
    (vec (for [n (range length)] (float (+ x (* n delta)))))))

(defn stretch-testing
  [ts [left right]]
  (if (= right (inc left))
    (nth ts left)
    (interpolate (nth ts left)
                 (nth ts right)
                 (- right left))))

;; TODO ensure that this works for even AND odd values
(defn fix-time-series
  [pred quality-coll value-coll]
  (flatten
   (vector (map (partial stretch-testing value-coll)
                (partition 2 1 (positions pred quality-coll)))
           (nth value-coll (last quality-coll)))))


