(ns forma.matrix.utils
  "A set of useful and general functions for matrix operations or
  filters."
  (:use [forma.utils :only (thrush)])
  (:require [incanter.core :as i]))

(defn is-square?
  "Returns true if input matrix is square, and false otherwise.

  Example usage:
    (is-square? (i/matrix (range 4) 2)) => true"
  [mat]
  {:pre [(instance? incanter.Matrix mat)]}
  (let [[row col] (i/dim mat)]
    (= row col)))

(defn singular?
  "Returns if square input matrix is singular, and false otherwise.

  Example usage:
    (singular? (i/matrix [[1 0] [1 0]])) => true
    (singular? (i/matrix [[1 0] [0 1]])) => false"
  [mat]
  {:pre [(is-square? mat)]}
  (<= (i/det mat) 0))

(defn transpose
  "Returns the transposition of a collection of vectors.

  Example usage:
    (transpose [[1 2 3] [4 5 6]]) => '([1 4] [2 5] [3 6])"
  [coll]
  (apply (partial map vector) coll))

(defn outer-product
  "Returns a flattened vector of the matrix product of a vector and its
  transpose.

  Example usage:
    (outer-product [1 2]) => '(1.0 2.0 2.0 4.0)"
  [coll]
  (let [mat (i/matrix coll)]
    (flatten (i/mmult mat (i/trans mat)))))

(defn insert-at
  "Inserts `ins-coll` into `coll` at the supplied `idx`.

  Example usage:
    (insert-at 1 [[2 1]] [4 5 6]) => [4 [2 1] 5 6]"
  [idx ins-coll coll]
  {:pre [(>= idx 0), (<= idx (count coll))]}
  (let [[begin end] (split-at idx coll)]
    (concat begin ins-coll end)))

(defn insert-into-val
  "Insert vector `xs` into a repeated sequence of the supplied
  length (filled with `val`) at the supplied insertion index.

  Example usage:
    (insert-into-val 0 1 4 [1 2 3]) => [0 1 2 3]"
  [val insertion-idx length xs]
  {:pre [(<= (+ (count xs) insertion-idx) length)]}
  (insert-at insertion-idx
             xs
             (-> length (- (count xs)) (repeat val))))

(defn pred-replace
  "Selectively replaces values in `coll` based on the return value of
  the supplied predicate. `pred` will receive each value in turn; a
  truthy return will trigger a replacement of the seq item with
  `val`.

  Example usage:
    (pred-replace #{:cake} 3 [1 2 :cake 4]) => '(1 2 3 4)"
  [pred val coll]
  (map #(if (pred %) val %) coll))

(defn logical-replace
  "Special version of `pred-replace` tailored for logical
  operators. `op` should be one of #{<, >, >=, <=, =}. if a value in
  the supplied collection returns true when compared to `compare-val`
  by `op`, `new-val` will be subbed into the sequence.

  Example usage:
    (logical-replace > 3 2 [1 2 5 6]) => (1 2 2 2)"
  [op compare-val new-val coll]
  (pred-replace #(op % compare-val) new-val coll))

(defn above-x?
  "Returns a partial function that accepts a single number. Returns
  true if that number of greater than `x`, false otherwise.

  Example usage:
    ((above-x? 5) 5) => falsey"
  [x]
  (partial < x))

(defn coll-avg
  "Returns the average value of the supplied collection of numbers.

  Example usage:
    (coll-avg [1 2 3 4]) => 2.5"
  [coll]
  {:pre [(pos? (count coll))]}
  (float (/ (reduce + coll) (count coll))))

(defn revolve
  "Returns a lazy sequence of revolutions of the supplied
  collection. (Note that the entire sequence must be realized, as the
  first item is cycled to the end for each revolution.)

  Example usage:
    (rotate [1 2 3]) => ([1 2 3] [2 3 1] [3 1 2])"
  [coll]
  (let [ct (count coll)]
    (->> coll cycle (partition ct 1) (take ct))))

(defn sparse-expander
  "Accepts a sequence of 2-tuples of the form `<idx, val>` and
  generates a sparse expansion with each `val` inserted at its
  corresponding `idx`. Missing values will be set to the supplied
  placeholder.

  If no starting index is supplied, `sparse-expander` assumes that
  counting begins with the first `<idx, val>` pair.

  Example usage:
    (sparse-expander 0 [[10 1] [13 8]]) => [1 0 0 8]
    (sparse-expander 0 [[10 1] [13 8]] :start 9) => [0 1 0 0 8]"
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

(defn sparse-prep
  "Cleans up data for use with `sparse-expander`, removing Cascalog
   nesting and weaving separate index and value fields into
   index-value tuples.

  Usage:
    (sparse-prep [3 2] [1 1]])
    ;=> ([3 1] [2 1])

    (sparse-prep [[3 2]] [[1 1]]])
    ;=> ([3 1] [2 1])

    Timeseries with mismatched lengths like (sparse-prep [[3 2]] [[1 1 1]])
    are caught by a precondition."
  [idxs vals]
  {:pre [(= (count (flatten idxs))
            (count (flatten vals)))]}
  (let [idxs (flatten idxs)
        vals (flatten vals)]
    (map vec (partition 2 2 (interleave idxs vals)))))

(defn sparsify
  "Wrapper for `sparse-expander` handles data coming in via a
   `defbufferop`. `sparse-expander` then makes a complete timeseries by
   inserting a nodata value when a period is missing.

  Usage:
    (sparsify 1 -9999 [3 5] [[-9999 3 4 5] [-9999 5 5 6]])
    ;=> [3 -9999 5]

    (sparsify 2 -9999 [3 5] [[-9999 3 4 5] [-9999 5 5 6]])
    ;=> [4 -9999 5]"
  [field-idx nodata idxs sorted-tuples]
  (->> (vec (map #(nth % field-idx) sorted-tuples))
       (sparse-prep idxs)
       (sparse-expander nodata)))

(defn idx->rowcol
  "Takes an index within a row vector, and returns the appropriate row
  and column within a matrix with the supplied dimensions. If only one
  dimension is supplied, assumes a square matrix.

  Example usage:
    (idx->rowcol 10 2) => [0 2]
    (idx->rowcol 10 4 18) => [4 2]"
  ([edge idx]
     {:pre [(pos? edge), (pos? (inc idx))]}
     (idx->rowcol edge edge idx))
  ([nrows ncols idx]
     {:pre [(pos? nrows), (pos? ncols), (pos? (inc idx))]}
     (let [max-idx (dec (* nrows ncols))]
       (if (<= idx max-idx)
         ((juxt #(quot % ncols) #(mod % ncols)) idx)))))

(defn rowcol->idx
  "For the supplied column and row in a rectangular matrix of
  dimensions (height, width), returns the corresponding index within a
  row vector of size (* width height). If only one dimension is
  supplied, assumes a square matrix.

  Example usage:
    (rowcol->idx 3 4 2 2) => 10
    (rowcol->idx 3 4 2 3) => 11
    (rowcol->idx 3 4 2 4) => nil"
  ([edge row col]
     (rowcol->idx edge edge row col))
  ([nrows ncols row col]
     {:pre [(pos? nrows), (pos? (inc row))
            (pos? ncols), (pos? (inc col))]}
     (let [max-idx (dec (* nrows ncols))
           idx (+ col (* ncols row))]
       (if (<= idx max-idx) idx))))

;; ## Multi-Dimensional Matrix Operations

(defn column-matrix
  "Returns an incanter column matrix of length `cols` filled with
  `val`.

  Example usage:
    (column-matrix 4 3) => [4.0 4.0 4.0]"
  [val cols]
  {:pre [(>= cols 0), (number? val)]}
  (i/matrix val cols 1))

(def ones-column
  "Convenient wrapper for column-matrix to create a matrix of ones.

  Example usage:
    (ones-column 4) => [1.0 1.0 1.0 1.0]"
  (partial column-matrix 1))

(defn matrix-of
  "Returns a matrix of dimension rows-by-cols with all elements equal
  to `val`.

  Example usage:
    (matrix-of 2 1 4) => [2 2 2 2]
    (matrix-of 0 2 2) => [[0 0] [0 0]]"
  [val rows cols]
  (apply thrush val
         (repeat rows (fn [v] (vec (repeat cols v))))))
