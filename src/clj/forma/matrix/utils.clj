(ns forma.matrix.utils
  (:require [incanter.core :as i]))

;; Useful general functions for matrix operations or filters. The
;; first functions should be very simple functions used as composites
;; or filters.  The latter functions should be matrix operations that
;; deal with arbitrarily large, multi-dimensional matrices.

(defn pred-replace
  "Selectively replaces values in `v` based on the return value of the
 supplied predicate. `pred` will receive each value in turn; a truthy
 return will trigger a replacement of the seq item with `new-val`."
  [v pred new-val]
  (map #(if (pred %) new-val %) v))

(defn boolean-replace
  "Special version of `pred-replace` tailored for boolean
operators. `pred` should be one of (<, >, >=, <=, =). if a value in
the supplied vector returns true when compared to `compare-val` by
pred, `new-val` will be subbed into the sequence.

    Example usage:

    (boolean-replace [1 2 5 6] > 3 2)
    ;=> (1 2 2 2)"

  [v pred compare-val new-val]
  (pred-replace v #(pred % compare-val) new-val))

(defn above-x? [x] (partial < x))

(defn average
  "average of a list"
  [lst] 
  (float (/ (reduce + lst) (count lst))))

(defn sparse-vector
  "Takes in a sequence of 2-tuples of the form `<idx, val>` and
  generates a sparse vector with each `val` inserted at its
  corresponding `idx`. Missing values will be set to the supplied
  placeholder."
  [size tuples placeholder]
  (loop [idx 0
         tup-seq tuples
         v (transient [])]
    (let [[pos val] (first tup-seq)]
      (cond (or (> idx size)
                (empty? tup-seq)) (persistent! v)
                (= idx pos) (recur (inc idx) (rest tup-seq) (conj! v val))
                :else       (recur (inc idx) tup-seq (conj! v placeholder))))))

;; Multi-dimensional matrix operations

(defn variance-matrix
  "construct a variance-covariance matrix from a given matrix `X`. If the
  value of the matrix multiplication yields an integer value, then we
  vectorize.  This would be equivalent to the var function in incanter.stats."
  [X]
  (let [product (i/mmult (i/trans X) X)]
    (i/solve (if (seq? product)
               product
               (vector product)))))

(defn column-matrix
  "create an incanter column matrix of the supplied value with length
  `x`."
  [val cols]
  (i/matrix val cols 1))

(def ones-column (partial column-matrix 1))

