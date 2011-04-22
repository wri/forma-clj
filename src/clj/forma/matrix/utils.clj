(ns forma.matrix.utils
  (:require [incanter.core :as i]))

;; Useful general functions 

(defn variance-matrix
  "construct a variance-covariance matrix from a given matrix X"
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

(defn above-x? [x] (partial < x))

(defn replace-val
  "replaces values in a vector [vec] that are (<, >, >=, <=, =) to [in-val]
  with the value supplied in [out-val]"
  [vec rel in-val out-val]
  (map #(if (rel % in-val) out-val %) vec))

(defn replace-in
 "replace the value in coll at index idx with value val"
  [coll idx val]
  {:post [(= (count coll) (count %))]}
  (concat (take idx coll) (list val) (drop (inc idx) coll)))

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
