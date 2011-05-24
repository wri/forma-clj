(ns forma.matrix.utils
  (:require [incanter.core :as i]))

;; Useful general functions for matrix operations or filters. The
;; first functions should be very simple functions used as composites
;; or filters.  The latter functions should be matrix operations that
;; deal with arbitrarily large, multi-dimensional matrices.

(defn insert-at
  "insert list [b] into list [a] at index [idx]."
  [idx a b]
  (let [[beg end] (split-at idx a)]
    (concat beg b end)))

(defn insert-into-val
  "insert vector `xs` into a repeated sequence of the supplied
  length (filled with `val`) at the supplied insertion index."
  [val insertion-idx length xs]
  (insert-at insertion-idx
             (-> length (- (count xs)) (repeat val))
             xs))

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

(defn sparse-expander
  "Takes in a sequence of 2-tuples of the form `<idx, val>` and
  generates a sparse expansion with each `val` inserted at its
  corresponding `idx`. Missing values will be set to the supplied
  placeholder.

  If no starting index is supplied, `sparse-expander` assumes that
  counting begins with the first `<idx, val>` pair."
  ([placeholder tuples & {:keys [start length]}]   
     (let [start  (or start (ffirst tuples))
           halt?  (fn [idx tup-seq]
                    (if length
                      (>= idx (+ start length))
                      (empty? tup-seq)))]
       (loop [idx start
              tup-seq tuples
              v (transient [])]
         (let [[[pos val] & more] tup-seq]
           (cond (halt? idx tup-seq) (persistent! v)
                 (when pos (= idx pos)) (recur (inc idx) more (conj! v val))
                 (when pos (> idx pos)) (recur (inc idx) more (conj! v placeholder))
                 :else (recur (inc idx) tup-seq (conj! v placeholder))))))))

(defn idx->rowcol
  "Takes an index within a row vector, and returns the appropriate
  row and column within a matrix with the supplied dimensions. If only
  one dimension is supplied, assumes a square matrix."
  ([edge idx]
     (idx->rowcol edge edge idx))
  ([nrows ncols idx]
     {:pre [(< idx (* nrows ncols)), (not (neg? idx))]
      :post [(and (< (first %) ncols)
                  (< (second %) nrows))]}
     ((juxt #(quot % ncols) #(mod % ncols)) idx)))

(defn rowcol->idx
  "For the supplied column and row in a rectangular matrix of
dimensions (height, width), returns the corresponding index within a
row vector of size (* width height). If only one dimension is
supplied, assumes a square matrix."
  ([edge row col]
     (rowcol->idx edge edge row col))
  ([nrows ncols row col]
     {:pre [(not (or (>= row nrows)
                     (>= col ncols)))]
      :post [(< % (* nrows ncols))]}
     (+ col (* ncols row))))

;; ## Multi-Dimensional Matrix Operations

(defn variance-matrix
  "construct a variance-covariance matrix from a given matrix `X`. If
  the value of the matrix multiplication yields an integer value, then
  we vectorize.  This would be equivalent to the var function in
  incanter.stats."
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
  "Returns a `dims`-dimensional matrix of `val`, with edge length of
  `edge`."
  [val dims edge]
  (reduce #(%2 %1)
          val
          (repeat dims (fn [v] (vec (repeat edge v))))))
