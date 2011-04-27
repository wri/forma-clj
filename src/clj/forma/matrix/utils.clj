(ns forma.matrix.utils
  (:require [incanter.core :as i]))

;; Useful general functions for matrix operations or filters. The
;; first functions should be very simple functions used as composites
;; or filters.  The latter functions should be matrix operations that
;; deal with arbitrarily large, multi-dimensional matrices.

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

(defn pred-replace
  "Selectively replaces values in `v` based on the return value of the
 supplied predicate. `pred` will receive each value in turn; a truthy
 return will trigger a replacement of the seq item with `new-val`."
  [v pred new-val]
  (map #(if (pred %) new-val %) v))

(defn logical-replace
  "Special version of `pred-replace` tailored for logical
operators. `pred` should be one of (<, >, >=, <=, =). if a value in
the supplied vector returns true when compared to `compare-val` by
pred, `new-val` will be subbed into the sequence.

    Example usage:

    (logical-replace [1 2 5 6] > 3 2)
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
  [size placeholder tuples]
  (loop [idx 0
         tup-seq tuples
         v (transient [])]
    (let [[pos val] (first tup-seq)]
      (cond (or (>= idx size)) (persistent! v)
            (= idx pos) (recur (inc idx) (rest tup-seq) (conj! v val))
            :else       (recur (inc idx) tup-seq (conj! v placeholder))))))

(defn idx->colrow
  "Takes an index within a row vector, and returns the appropriate
  column and row within a matrix with the supplied dimensions. If only
  one dimension is supplied, assumes a square matrix."
  ([edge idx]
     (idx->colrow edge edge idx))
  ([width height idx]
     {:pre [(< idx (* width height)), (not (neg? idx))]
      :post [(and (< (first %) width)
                  (< (second %) height))]}
     ((juxt #(mod % width) #(quot % width)) idx)))

(defn colrow->idx
  "For the supplied column and row in a rectangular matrix of
dimensions (height, width), returns the corresponding index within a
row vector of size (* width height). If only one dimension is
supplied, assumes a square matrix."
  ([edge col row]
     (colrow->idx edge edge col row))
  ([width height col row]
     {:post [(< % (* width height))]}
     (+ col (* width row))))

;; ## Multi-dimensional matrix operations

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

(defn matrix-of
  "Returns an n-dimensional matrix of `val`, with edge length of
  `edge`."
  [val dims edge]
  (reduce #(%2 %1)
          val
          (repeat dims (fn [v] (vec (repeat edge v))))))
