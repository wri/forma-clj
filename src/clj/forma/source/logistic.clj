(ns forma.source.logistic
  (:require [incanter.core :as i])
  (:import [org.jblas FloatMatrix MatrixFunctions Solve DoubleMatrix])
  (:use [clojure-csv.core]))

;; TODO: function to correct for error induced by ridge

;; TODO: function to calculate probabilities, given features and
;; parameter vector

(defn grab-csv-data
  [file-name]
  (map
   (partial map #(Float/parseFloat %))
   (parse-csv
    (slurp file-name))))

(def y (apply
        concat
        (take 100000 (grab-csv-data "/Users/danhammer/Desktop/myslab.csv"))))
(def X (take 100000
             (map (partial cons 1)
                  (grab-csv-data "/Users/danhammer/Desktop/mys.csv"))))
(def beta (repeat
           (count (first X))
           0))

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

;; (defn matrix-mult
;;   "returns the (n x n) matrix associated with multiplying an (n x m)
;;   matrix with an (m x n) matrix"
;;   [mat1 mat2]
;;   (let [row-mult (fn [mat row]
;;                    (map (partial dot-product row)
;;                         (transpose mat)))]
;;     (map (partial row-mult mat2)
         ;; mat1)))

(defn to-float-matrix
  [mat]
  (DoubleMatrix.
   (into-array (map double-array mat))))

;; (defn convert-float-matrix)
;; (def a [[14 9 3] [2 11 15] [0 12 17] [5 2 3]])
;; (.mulRow (to-float-matrix a) (Integer. 1) (Integer. 1))
;; (map vec (to-array (.toArray2 (to-float-matrix a))))
;; (map vec (.toArray2 (to-float-matrix a)))
;; (.sum (FloatMatrix. (float-array y)))
;; (def fy (FloatMatrix. (float-array y)))
;; (.sum fy)

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
  (reduce + (map (partial log-likelihood beta-seq) label-seq feature-mat)))

(defn probability-calc
  "returns a vector of probabilities for each observation"
  [beta-seq feature-mat]
  (map (partial logistic-prob beta-seq)
       feature-mat))

(defn score-seq
  "returns the scores for each parameter"
  [beta-seq label-seq feature-mat]
  (let [prob-seq (probability-calc beta-seq feature-mat)
        features (to-float-matrix feature-mat)]
    (.mmul (.transpose features)
           (DoubleMatrix. (double-array (map - label-seq prob-seq))))))

(defn tester-mult
  [n afn]
  (let [y (take n (make-binary mys-labels))
        X (map (partial cons 1) (take n mys-features))
        beta (repeat 23 0)]
    (time (.rows (afn beta y X)))))

(defn wishful-func
  [coll mat]
  (map (partial map * coll) mat))

(defn info-matrix
  "returns the square information matrix for the logistic probability
  function; the dimension is given by the number of features"
  [beta-seq labels feature-mat]
  (let [mult-func (fn [x] (* x (- 1 x)))
        prob-seq  (->> (probability-calc beta-seq feature-mat)
                       (map mult-func))
        scale-feat (wishful-func prob-seq (transpose feature-mat))]
    (.mmul (to-float-matrix scale-feat) (to-float-matrix feature-mat))))

;; (.mmul (to-float-matrix scale-feat) (to-float-matrix feature-mat))

;; (defn info-matrix
;;   "returns the square information matrix for the logistic probability
;;   function; the dimension is given by the number of features"
;;   [beta-seq labels feature-mat]
;;   (let [mult-func (fn [x] (* x (- 1 x)))
;;         prob-seq  (->> (probability-calc beta-seq feature-mat)
;;                        (map mult-func))]
;;     (i/mmult (map scaled-vector prob-seq (i/trans features))
;;              features)))

(defn beta-update
  "returns a vector of updates for the parameter vector; the
  ridge-constant is a very small scalar, used to ensure that the
  inverted information matrix is non-singular."
  [beta-seq labels features rdg-cons]
  (let [num-features (count beta-seq)
        info-adj (.addi
                  (info-matrix beta-seq labels features)
                  (.muli (DoubleMatrix/eye (int num-features)) (float rdg-cons)))]
    (.mmul (Solve/solve info-adj (DoubleMatrix/eye (int 20)))
           (score-seq beta-seq labels features))))

;; (println )))
    ;; (.mmul
    ;;  (Solve/solve info-adj (DoubleMatrix/eye (int 20)))
    ;;  

;; (def update1 (vec (.toArray (beta-update beta y X 0.0000001))))
;; (def beta2 (map + beta update1))
;; (def update2 (vec (.toArray (beta-update beta2 y X 0.0000001))))
;; (def beta3 (map + beta2 update2))

;; (.muli (FloatMatrix/eye (Integer. 2)) (float 2))

 ;; (defn beta-update
 ;;  "returns a vector of updates for the parameter vector; the
 ;;  ridge-constant is a very small scalar, used to ensure that the
 ;;  inverted information matrix is non-singular."
 ;;  [beta-seq labels
 ;;  features rdg-cons]
 ;;  (let [num-features (count beta-seq)
 ;;        info-adj (i/plus
 ;;                  (info-matrix beta-seq labels features)
 ;;                  (i/diag (repeat num-features rdg-cons)))]
 ;;    (i/mmult
 ;;     (i/solve info-adj)
 ;;     (score-seq beta-seq labels features))))

(defn logistic-beta-vector
  "return the parameter vector used to "
  [labels features rdg-cons]
  (let [b (repeat 20 0)]
    (loop [beta b iter 10]
      (if (zero? iter)
        beta
        (recur (let [update (beta-update beta labels features rdg-cons)]
                 (map + beta (vec (.toArray update))))
               (dec iter))))))



