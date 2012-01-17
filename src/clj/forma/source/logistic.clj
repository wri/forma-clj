(ns forma.source.logistic
  (:require [incanter.core :as i]))

;; TODO: function to correct for error induced by ridge

;; TODO: function to calculate probabilities, given features and
;; parameter vector

(defn scaled-vector
  "returns a vector, where each element is scaled by `scalar`"
  [scalar coll]
  (map #(* scalar %) coll))

(defn logistic-fn
  "returns the value of the logistic function, given input `x`"
  [x]
  (let [exp-x (Math/exp x)]
    (/ exp-x (inc exp-x))))

(defn dot-product
  "returns the dot product of two vectors"
  [x y]
  (reduce + (map * x y)))

(defn transpose
  "returns the transposition of a `coll` of vectors"
  [coll]
  (apply map vector coll))

(defn matrix-mult
  "returns the (n x n) matrix associated with multiplying an (n x m)
  matrix with an (m x n) matrix"
  [mat1 mat2]
  (let [row-mult (fn [mat row]
                   (map (partial dot-product row)
                        (transpose mat)))]
    (map (partial row-mult mat2)
         mat1)))

(defn logistic-prob
  "returns the probability of a binary outcome given a parameter
  vector `beta-seq` and a feature vector for a given observation"
  [beta-seq x]
  (logistic-fn (dot-product beta-seq x)))

(defn log-likelihood
  "returns the log likelihood of a given pixel, conditional on its
  label (0-1) and the probability of label equal to 1."
  [beta-seq label x]
  (let [prob (logistic-prob beta-seq x)]
    (+ (* label (Math/log prob))
       (* (- 1 label) (Math/log (- 1 prob))))))

(defn total-log-likelihood
  "returns the total log likelihood for a group of pixels; input
  labels and features for the group of pixels, aligned correctly so
  that the first label and feature correspond to the first pixel."
  [beta-seq labels features]
  (reduce + (map (partial log-likelihood beta-seq) labels features)))

(defn probability-calc
  "returns a vector of probabilities for each observation"
  [beta-seq feature-seqs]
  (map (partial logistic-prob beta-seq)
       feature-seqs))

(defn score-seq
  "returns the scores for each parameter"
  [beta-seq labels features]
  (let [prob-seq (probability-calc beta-seq features)]
    (matrix-mult (i/trans features) (map - labels prob-seq))))

(defn info-matrix
  "returns the square information matrix for the logistic probability
  function; the dimension is given by the number of features"
  [beta-seq labels features]
  (let [mult-func (fn [x] (* x (- 1 x)))
        prob-seq  (->> (probability-calc beta-seq features)
                       (map mult-func))]
    (i/mmult (map scaled-vector prob-seq (i/trans features))
             features)))

(defn beta-update
  "returns a vector of updates for the parameter vector; the
  ridge-constant is a very small scalar, used to ensure that the
  inverted information matrix is non-singular."
  [beta-seq labels
  features rdg-cons]
  (let [num-features (count beta-seq)
        info-adj (i/plus
                  (info-matrix beta-seq labels features)
                  (i/diag (repeat num-features rdg-cons)))]
    (i/mmult
     (i/solve info-adj)
     (score-seq beta-seq labels features))))

(defn logistic-beta-vector
  "return the parameter vector used to "
  [labels features rdg-cons]
  (let [b (repeat 23 0)]
    (loop [beta b iter 9]
      (if (zero? iter)
        beta
        (recur (let [update (beta-update beta labels features rdg-cons)]
                 (map + beta update))
               (dec iter))))))



