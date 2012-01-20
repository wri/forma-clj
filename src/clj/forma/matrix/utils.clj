(ns forma.matrix.utils
  "Useful general functions for matrix operations or filters."
  (:use [forma.utils :only (thrush)])
  (:require [incanter.core :as i]))

;; The first functions should be very simple functions used as
;; composites or filters.  The latter functions should be matrix
;; operations that deal with arbitrarily large, multi-dimensional
;; matrices.

(defn is-square?
  "returns true if input matrix is square, and false otherwise"
  [mat]
  (let [[row col] (i/dim mat)]
    (= row col)))

(defn singular?
  "returns if square input matrix is singular, and false otherwise"
  [mat]
  {:pre [(is-square? mat)]}
  (<= (i/det mat) 0))

(defn transpose
  "returns the transposition of a `coll` of vectors"
  [coll]
  (apply (partial map vector) coll))

(defn outer-product
  "returns a flattened vector of the outer product of a vector and its
  transpose"
  [coll]
  (let [mat (i/matrix coll)]
    (flatten (i/mmult mat (i/trans mat)))))

(defn insert-at
  "Inserts `ins-coll` into `coll` at the supplied `idx`."
  [idx ins-coll coll]
  {:pre [(>= idx 0), (<= idx (count coll))]}
  (let [[beg end] (split-at idx coll)]
    (concat beg ins-coll end)))

(defn insert-into-val
  "insert vector `xs` into a repeated sequence of the supplied
  length (filled with `val`) at the supplied insertion index."
  [val insertion-idx length xs]
  {:pre [(<= (+ (count xs) insertion-idx) length)]}
  (insert-at insertion-idx
             xs
             (-> length (- (count xs)) (repeat val))))

(defn pred-replace
  "Selectively replaces values in `coll` based on the return value of the
 supplied predicate. `pred` will receive each value in turn; a truthy
 return will trigger a replacement of the seq item with `val`."
  [pred val coll]
  (map #(if (pred %) val %) coll))

(defn logical-replace
  "Special version of `pred-replace` tailored for logical
operators. `op` should be one of #{<, >, >=, <=, =}. if a value in the
supplied collection returns true when compared to `compare-val` by
`op`, `new-val` will be subbed into the sequence.

    Example usage:

    (logical-replace > 3 2 [1 2 5 6])
    ;=> (1 2 2 2)"
  [op compare-val new-val coll]
  (pred-replace #(op % compare-val) new-val coll))

(defn above-x?
  "Returns a partial function that accepts a single number. Returns
  true if that number of greater than `x`, false otherwise."
  [x] (partial < x))

(defn coll-avg
  "Returns the average value of the supplied collection of numbers."
  [coll]
  {:pre [(seq coll)]}
  (float (/ (reduce + coll) (count coll))))

(defn revolve
  "Returns a lazy sequence of revolutions of the supplied
  collection. (Note that the entire sequence must be realized, as the
  first item is cycled to the end for each revolution.) For example:

    (rotate [1 2 3])
    ;=> ([1 2 3] [2 3 1] [3 1 2])"
  [coll]
  (let [ct (count coll)]
    (->> coll cycle (partition ct 1) (take ct))))

(defn sparse-expander
  "Takes in a sequence of 2-tuples of the form `<idx, val>` and
  generates a sparse expansion with each `val` inserted at its
  corresponding `idx`. Missing values will be set to the supplied
  placeholder.

  If no starting index is supplied, `sparse-expander` assumes that
  counting begins with the first `<idx, val>` pair."
  [placeholder tuples & {:keys [start length]}]   
  (let [start (or start (ffirst tuples))
        halt? (fn [idx tup-seq]
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
              :else (recur (inc idx) tup-seq (conj! v placeholder)))))))

(defn idx->rowcol
  "Takes an index within a row vector, and returns the appropriate
  row and column within a matrix with the supplied dimensions. If only
  one dimension is supplied, assumes a square matrix."
  ([edge idx]
     (idx->rowcol edge edge idx))
  ([nrows ncols idx]
     {:pre [(< idx (* nrows ncols)), (not (neg? idx))]
      :post [(and (< (first %) nrows)
                  (< (second %) ncols))]}
     ((juxt #(quot % ncols) #(mod % ncols)) idx)))

(defn rowcol->idx
  "For the supplied column and row in a rectangular matrix of
dimensions (height, width), returns the corresponding index within a
row vector of size (* width height). If only one dimension is
supplied, assumes a square matrix."
  ([edge row col]
     (rowcol->idx edge edge row col))
  ([nrows ncols row col]
     {:pre [(not (or (neg? row), (>= row nrows)
                     (neg? col), (>= col ncols)))]
      :post [(< % (* nrows ncols))]}
     (+ col (* ncols row))))

;; ## Multi-Dimensional Matrix Operations

;; TODODAN: Pre and post conditions on this function, please. It fails
;; under a few cases... 1x2 matrices, for example. Also, please put in
;; some links about what the function's supposed to be doing, and
;; check the tests that I've returned.
;;
;; TODODAN: Also, please add some more tests!

(defn variance-matrix
  "construct a variance-covariance matrix from the supplied matrix
  `X`. (`X` can be an incanter matrix or a series of nested vectors.)
  If the value of the matrix multiplication yields an integer value,
  then we vectorize.  This is equivalent to
  `incanter.stats/variance`."
  [X]
  (let [product (i/mmult (i/trans X) X)]
    (i/solve (if (coll? product)
               product
               [product]))))

(defn column-matrix
  "Returns an incanter column matrix of length `cols` filled with
  `val`."
  [val cols]
  {:pre [(>= cols 0), (number? val)]}
  (i/matrix val cols 1))

(def ones-column
  (partial column-matrix 1))

(defn matrix-of
  "Returns a `dims`-dimensional matrix of `val`, with edge length of
  `edge`."
  [val dims edge]
  (apply thrush val
         (repeat dims (fn [v] (vec (repeat edge v))))))
