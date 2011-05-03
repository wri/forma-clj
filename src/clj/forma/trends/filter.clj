(ns forma.trends.filter
  (:use [forma.matrix.utils :only (ones-column
                                   average
                                   variance-matrix
                                   insert-at
                                   insert-into-zeros
                                   sparse-expander)]
        [clojure.contrib.seq :only (positions)])
  (:require [incanter.core :as i]))

;; These functions clean and deseasonalize time-series. The first few
;; functions are smaller in scope, and meant to be used in the
;; higher-order functions toward the end of this script.  The first
;; higher-order function is meant to smooth a time-series based on the
;; Hodrick-Prescott convention.  Later, the target (vegetation)
;; time-series is combined with a measure of reliability to
;; interpolate across "bad" values.

;; TODO: Pull out smaller functions into, perhaps, a utils namespace
;; to be used in other contexts, namespaces.   

;; De-seasonalize time-series

(defn seasonal-rows [n]
  "lazy sequence of monthly dummy vectors"
  (vec
   (take n
         (partition 11 1
                    (cycle (cons 1 (repeat 11 0)))))))

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
        trend-cond (i/solve
                    (i/plus coeff-matrix
                            (i/identity-matrix T)))]
    (i/mmult trend-cond ts)))

;; Interpolate unreliable values

(defn interpolate
  "calculate a linear interpolation between `x1` and `x2` with the specified
  `length` between them."
  [x1 x2 length]
  (let [delta (/ (- x2 x1) (dec length))]
    (vec
     (for [n (range length)]
       (float (+ x1 (* n delta)))))))

(defn stretch-ts
  "stretch time-series across bad values if the left and right values of a
  tuple are not sequential.  The original time-series is `ts` and the tuples
  are a moving window (from `partition 2 1`) based on the valid values from
  an associated time-series of reliability values."
  [ts [left right]]
  (if (= right (inc left))
    (nth ts left)
    (interpolate (nth ts left)
                 (nth ts right)
                 (- right left))))

(defn mask
  "create a new vector where values from `coll1` are only passed through
  if they satisfy the predicate `pred` for `coll2`.  All other values are
  set to nil."
  [pred coll1 coll2]
  {:pre [(= (count coll1) (count coll2))]}
  (map #(when-not (pred %2) %1) coll2 coll1))

(defn replace-index-set
  "replace values in `coll` with `new-val` for all indices in `idx-set`"
  [idx-set new-val coll]
  (for [[m n] (map-indexed vector coll)]
    (if (idx-set m) new-val n)))

(defn bad-ends
  "make a set of indices of a collection `coll` for which there are continuous
  *bad* values, given by the set of values in `bad-set` which serves as a
  predicate function to (effectively) filter `coll` on the ends.  If there are
  no bad values on either end, then the function will return an empty set."
  [bad-set coll]
  (let [m-coll (map-indexed vector coll)
        r-coll (reverse m-coll)]
    (set 
     (apply concat
            (map #(for [[m n] % :while (bad-set n)] m)
                 [m-coll r-coll])))))

(defn act-on-good
  "apply function `func` to all non-nil values within a collection `coll`"
  [func coll]
  (func
   (filter (complement nil?) coll)))

(defn neutralize-ends
  "replace the ends of a value-collection (like NDVI) if the ends are unreliable,
  according to an associated reliability index, manifest in `reli-test`. if there
  are no bad values (as indicated by `bad-set` values in `reli-coll`) then
  `neutralize-ends` will return the original time-series. `bad-set` is a set of
  `reli-coll` values that indicate unreliable pixels."
  [bad-set reli-coll val-coll]
  (let [avg (act-on-good average
                         (mask bad-set reli-coll val-coll))]
    (replace-index-set
     (bad-ends bad-set reli-coll)
     avg
     val-coll)))

;; TODO: This function works, but it's ugly.  Clean up. And put in
;; pre- and post-conditions.

(defn make-reliable
  "This function has two parts: (1) replace bad values at the ends with the
  average of the reliable values in the target coll, `value-coll`. (2) smooth
  over *bad* values, given by `bad-set`, which are determined based on the
  reliability (or quality) collection, `quality-coll`.  The `good-val` is a
  single value indicating good pixels in the quality collection.  This could,
  soon, be removed in place of a more general approach."
  [bad-set good-val quality-coll value-coll]
  (if (empty? (filter bad-set quality-coll)) nil
      (let [bad-end-set (bad-ends
                         bad-set
                         quality-coll)
            new-qual (replace-index-set
                      bad-end-set
                      good-val
                      quality-coll)
            new-vals (neutralize-ends
                      bad-set
                      quality-coll
                      value-coll)
            good-seq (positions
                      (complement bad-set)
                      new-qual)]
        (vec (flatten
              (vector (map (partial stretch-ts new-vals)
                           (partition 2 1 good-seq))
                      (nth new-vals (last good-seq))))))))

