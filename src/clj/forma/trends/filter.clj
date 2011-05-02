(ns forma.trends.filter
  (:use [forma.matrix.utils :only (ones-column
                                   average
                                   variance-matrix
                                   insert-at
                                   insert-into-zeros
                                   sparse-expander)]
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

;; TODO: ensure that this works for even AND odd values; and ensure
;; that the filter works with bad values at the end of the
;; time-series; and that it will still work if there are no bad
;; values, just returning the original time-series.

;; TODO: Make fix-time-series its own cascalog query, to allow for the
;; contingency of NO good values, which will ouytput nil.

(defn fix-time-series
  ""
  [pred quality-coll value-coll]
  (if (empty? (filter pred quality-coll)) nil
    (let [goodpos-seq (positions pred quality-coll)]
      (vec (flatten
            (vector (map (partial stretch-testing value-coll)
                         (partition 2 1 goodpos-seq))
                    (nth value-coll (last goodpos-seq))))))))


(defn mask
  "create a new vector where values from `coll1` are only passed through
  if they satisfy the predicate `pred` for `coll2`.  All other values are
  set to nil."
  [pred coll1 coll2]
  {:pre [(= (count coll1) (count coll2))]}
  (map #(when-not (pred %2) %1) coll2 coll1))

;; (defn fix-time-series
;;   [pred qual-coll val-coll]
;;   (vec
;;    (map-indexed vector (mask pred qual-coll val-coll))))


(defn replace-index-set
  [idx-set new-val coll]
  (for [[m n] (map-indexed vector coll)]
    (if (idx-set m) new-val n)))

(defn bad-ends
  "collect a set of the indices of bad ends. if the bad value is, say, 2, then the
  predicate would be #{2}"
  [pred coll]
  (let [m-coll (map-indexed vector coll)
        r-coll (reverse m-coll)]
    (set 
     (apply concat
            (map #(for [[m n] % :while (pred n)] m)
                 [m-coll r-coll])))))

(defn act-on-good
  [func coll]
  (func
   (filter (complement nil?) coll)))

(defn neutralize-ends
  [bad-set reli-coll val-coll]
  (let [avg (act-on-good average
                         (mask bad-set reli-coll val-coll))]
    (replace-index-set
     (bad-ends bad-set reli-coll)
     avg
     val-coll)))

(defn fix-time-series
  ""
  [bad-val-set quality-coll value-coll]
  (if (empty? (filter bad-val-set quality-coll)) nil
      (let [bad-end-set (bad-ends bad-val-set quality-coll)
            new-qual (replace-index-set bad-end-set 1 quality-coll)
            new-vals (neutralize-ends bad-val-set quality-coll value-coll)
            goodpos-seq (positions (complement bad-val-set) new-qual)]
        (vec (flatten
              (vector (map (partial stretch-testing new-vals)
                           (partition 2 1 goodpos-seq))
                      (nth new-vals (last goodpos-seq))))))))


(def reli-test [2 2 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2])

(def ndvi-test [7417 7568 7930 8049 8039 8533 8260 8192 7968 7148 7724 8800 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538 7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698 7838 8967 8136 7532 7838 8009 8136 8400 8219 8051 8091 7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813 8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216 7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195 8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041 7250 7884 8105 8033 8340 8288 7691 7599 8480 8563 8033 7708 7575 7996 7739 8058 7400 6682 7999 7655 7533 7904 8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878 8524 8167 7526 7330 7325 7485 8108 7978 7035 7650 4000])

