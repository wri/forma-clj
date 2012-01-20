(ns forma.source.logistic
  (:use [forma.utils :only (dot-product transpose multiply-rows)]
        [clojure.contrib.math :only (abs)])
  (:require [incanter.core :as i])
  (:import [org.jblas FloatMatrix MatrixFunctions Solve DoubleMatrix]))

;; TODO: correct for error induced by ridge

;; Namespace Conventions: Each observation is assigned a binary
;; `label` which indicates deforestation during the training period.
;; These labels are collected for a group of pixels into `label-seq`.
;; Each pixel also has a sequence of features, or `feature-seq`.  The
;; pixel is identified by the order its attributes appear in the
;; feature and label sequence.  That is, it is vital that the labels
;; and feature sequences are consistently positioned in the label and
;; feature collections.

(defn logistic-fn
  "returns the value of the logistic function, given input `x`"
  [x]
  (let [exp-x (Math/exp x)]
    (/ exp-x (inc exp-x))))

(defn to-double-matrix
  "returns a DoubleMatrix instance for use with jBLAS functions"
  [mat]
  (DoubleMatrix.
   (into-array (map double-array mat))))

(defn logistic-prob
  "returns the probability of a binary outcome given a parameter
  vector `beta-seq` and a feature vector for a given observation"
  [beta-seq feature-seq]
  (logistic-fn (dot-product beta-seq feature-seq)))

(defn log-likelihood
  "returns the log likelihood of a given pixel, conditional on its
  label (0-1) and the probability of label equal to 1."
  [beta-seq label feature-seq]
  (let [prob (logistic-prob beta-seq feature-seq)]
    (+ (* label (Math/log prob))
       (* (- 1 label) (Math/log (- 1 prob))))))

(defn total-log-likelihood
  "returns the total log likelihood for a group of pixels; input
  labels and features for the group of pixels, aligned correctly so
  that the first label and feature correspond to the first pixel."
  [beta-seq label-seq feature-mat]
  (reduce + (map (partial log-likelihood beta-seq)
                 label-seq
                 feature-mat)))

(defn probability-calc
  "returns a vector of probabilities for each observation"
  [beta-seq feature-mat]
  (map (partial logistic-prob beta-seq)
       feature-mat))

(defn score-seq
  "returns the scores for each parameter"
  [beta-seq label-seq feature-mat]
  (let [prob-seq (probability-calc beta-seq feature-mat)
        features (to-double-matrix feature-mat)]
    (.mmul (.transpose features)
           (DoubleMatrix.
            (double-array
             (map - label-seq prob-seq))))))

(defn info-matrix
  "returns the square information matrix for the logistic probability
  function; the dimension is given by the number of features"
  [beta-seq feature-mat]
  (let [mult-func (fn [x] (* x (- 1 x)))
        prob-seq  (->> (probability-calc beta-seq feature-mat)
                       (map mult-func))
        scale-feat (multiply-rows
                    prob-seq
                    (transpose feature-mat))]
    (.mmul (to-double-matrix scale-feat)
           (to-double-matrix feature-mat))))

(defn beta-update
  "returns a vector of updates for the parameter vector; the
  ridge-constant is a very small scalar, used to ensure that the
  inverted information matrix is non-singular."
  [beta-seq label-seq feature-mat rdg-cons]
  (let [num-features (count beta-seq)
        info-adj (.addi
                  (info-matrix beta-seq feature-mat)
                  (.muli (DoubleMatrix/eye (int num-features))
                         (float rdg-cons)))]
    (vec (.toArray
          (.mmul (Solve/solve
                  info-adj
                  (DoubleMatrix/eye (int num-features)))
                 (score-seq beta-seq label-seq feature-mat))))))

(defn logistic-beta-vector
  "return the estimated parameter vector; which is used, in turn, to
  calculate the estimated probability of the binary label"
  [label-seq feature-mat rdg-cons converge-threshold max-iter]
  (let [beta-init (repeat (count (first feature-mat)) 0)]
    (loop [beta beta-init
           iter max-iter
           beta-diff 100]
      (if (or
           (zero? iter)
           (< beta-diff converge-threshold))
        beta
        (let [update (beta-update beta label-seq feature-mat rdg-cons)
              beta-new (map + beta update)
              diff (reduce + (map (comp abs -) beta beta-new))]
          (recur
           beta-new
           (dec iter)
           diff))))))

(defn estimated-probabilities
  "returns the set of probabilities, after applying the parameter
  values estimated over the training data; both `label-seq` and
  `traning-features` reflect data over the training period and
  `updated-features` reflect data through some interval, which could
  be the end of the training period (for internal validataion) but is
  most likely some interval thereafter"
  [label-seq training-features updated-features]
  (let [new-beta (logistic-beta-vector
                  label-seq
                  training-features
                  1e-8
                  1e-6
                  250)]
    (probability-calc new-beta updated-features)))


