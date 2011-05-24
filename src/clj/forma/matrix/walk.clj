(ns forma.matrix.walk
  (:use cascalog.api
      [clojure.contrib.math :only (floor)]
      [forma.matrix.utils :only (insert-into-val)]))

(defn walk-matrix
  "Walks along the rows and columns of a matrix at the given window
  size, returning all (window x window) snapshots."
  [m window]
  (mapcat (comp
           (partial apply map vector)
           (partial map (partial partition window 1)))
          (partition window 1 m)))

(defn buffer-matrix
  "create a buffer of length `buf` around rectangular matrix `mat`, with
  elements equal to `nil-val`"
  [buf nil-val mat]
  (let [new-w   (->> buf (* 2) (+ (count (first mat))))
        buf-row (vec (repeat new-w nil-val))]
    (into (->> mat
               (map (partial insert-into-val buf new-w nil-val))
               (apply conj [buf-row]))
          [buf-row])))

(defn windowed-function
  "apply a function `fn` to each element in a matrix `mat` over a moving
  window, defined by the number of neighbors."
  [f num-neighbors mat]
  {:pre [(> (count mat) (+ 1 (* 2 num-neighbors)))]}
  (let [window  (+ 1 (* 2 num-neighbors))
        new-mat (buffer-matrix num-neighbors nil mat)]
    (map (comp
          (partial apply f)
          (partial filter #(not= nil %))
          flatten)
         (walk-matrix new-mat window))))
