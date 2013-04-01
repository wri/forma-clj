(ns forma.classify.logistic
  (:use [forma.utils]
        [clojure.math.numeric-tower :only (abs)]
        [forma.matrix.utils]
        [cascalog.api])
(:require [incanter.core :as i]
          [cascalog.ops :as c])
  (:import [org.jblas FloatMatrix MatrixFunctions Solve DoubleMatrix]))

;; Namespace Conventions: Each observation is assigned a binary
;; `label` which indicates deforestation during the training period.
;; These labels are collected for a group of pixels into `label-row`.
;; Each pixel also has a matrix of features, or `feature-mat`.  The
;; pixel is identified by the order its attributes appear in the
;; feature and label sequence.  That is, it is vital that the labels
;; and feature sequences are consistently positioned in the label and
;; feature collections.

(defn ^DoubleMatrix
  to-double-matrix
  "converts a clojure matrix representation to a DoubleMatrix instance
  for use with jBLAS functions

  Argument:
    vector of vectors; clojure representation of a matrix

  Example:
    (to-double-matrix [0]) => #<DoubleMatrix []>
    (to-double-matrix [[0]]) => #<DoubleMatrix [0.0]>
    (to-double-matrix [[0] [1]]) => #<DoubleMatrix [0.0; 1.0]>

  Reference:
    http://jblas.org/javadoc/org/jblas/DoubleMatrix.html"
  [mat]
  (DoubleMatrix.
   (into-array (map double-array mat))))

(defn ^DoubleMatrix
  to-double-rowmat
  "converts a clojure vector to a DoubleMatrix row vector

  Argument:
    persistent vector

  Example:
    (to-double-rowmat [1 2 3]) => #<DoubleMatrix [1.0, 2.0, 3.0]>

  Reference:
    http://jblas.org/javadoc/org/jblas/DoubleMatrix.html"
  [coll]
  (to-double-matrix [(vec coll)]))

(defn ^DoubleMatrix
  logistic-fn
  "returns the value of the logistic curve at supplied `x` value;
  output is a DoubleMatrix (org.jblas.DoubleMatrix).

  Argument:
    DoubleMatrix instance (org.jblas.DoubleMatrix) with only one
    value

  Example:
    (logistic-fn (to-double-rowmat [0])) => #<DoubleMatrix [0.5]>

  Reference:
    http://en.wikipedia.org/wiki/Logistic_function"
  [x]
  (let [exp-x (MatrixFunctions/exp x)
        one (DoubleMatrix/ones 1)]
    (.divi exp-x (.add exp-x one))))


(defn ^DoubleMatrix
  logistic-prob
  "returns the DoubleMatrix value of the logistic curve at the value
  given by the dot product of the parameter vector and the feature
  vector. Both arguments and return values are DoubleMatrix instances.

  Precondition checks for empty beta/feature row matrices, which are
  created when `to-double-rowmat` operates on `nil`. This should never
  happen in production, but if it does, the result of classification
  would always be a probability of 0.5.

  Arguments: 
    beta-rowmat: DoubleMatrix row vector
    features-rowmat: DoubleMatrix row vector, of equal length as
                     beta-rowmat

  Example:
    (def beta (to-double-rowmat [0 0]))
    (def features (to-double-rowmat [1 2]))
    (logistic-prob beta features) => #<DoubleMatrix [0.5]>"
  [beta-rowmat features-rowmat]
  ;; check for empty double matrices (created with `nil`)
  {:pre [(not (= beta-rowmat (to-double-rowmat nil)))
         (not (= features-rowmat (to-double-rowmat nil)))]}
  (logistic-fn
   (to-double-rowmat [(.dot beta-rowmat features-rowmat)])))

;; TODO: convert the functions that are useful but not actually used
;; in this version of estimation, including `log-likelihood` and
;; `total-log-likelihood`.

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

(defn ^DoubleMatrix
  probability-calc
  "returns a DoubleMatrix row vector of probabilities given by the
  in-place dot product (read: matrix multiplication) of the beta
  vector and the feature vectors collected in the feature matrix. Both
  arguments and return values are DoubleMatrix instances.

  Example:
    (def beta (to-double-rowmat [0 0]))
    (def feat (to-double-matrix [[1 2] [4 5]]))
    (probability-calc beta feat) => #<DoubleMatrix [0.5, 0.5]>"
  [beta-rowmat feature-mat]
  (let [prob-row (.mmul beta-rowmat (.transpose feature-mat))]
    (logistic-fn prob-row)))

(defn ^DoubleMatrix
  score-seq
  "returns the score for each parameter at the current iteration,
  based on the estimated probabilities and the true labels. Both
  arguments and return values are DoubleMatrix instances.

  Example:
    (def beta (to-double-rowmat [0 0]))
    (def feat (to-double-matrix [[1 2] [4 5]]))
    (def label (to-double-rowmat [1 0]))
    (score-seq beta label feat) => #<DoubleMatrix [-1.5; -1.5]>

  Reference:
    http://en.wikipedia.org/wiki/Scoring_algorithm"
  [beta labels feature-mat]
    (let [prob-seq (probability-calc beta feature-mat)]
    (.mmul (.transpose feature-mat)
           (.transpose (.sub labels prob-seq)))))

(defn ^DoubleMatrix
  bernoulli-var
  "returns the variance of a bernoulli random variable, given a row
  matrix of probabilities `row-prob`.  Both arguments and return
  values are DoubleMatrix instances.

  Example:
    (def row-prob (to-double-rowmat [0.5 0.5]))
    (bernoulli-var row-prob) => #<DoubleMatrix [0.25, 0.25]>

  Reference:
    http://en.wikipedia.org/wiki/Bernoulli_distribution"
  [row-prob]
  (let [ones (DoubleMatrix/ones (.rows row-prob) (.columns row-prob))
        new-mat (.add ones (.neg row-prob))]
    (.mul row-prob new-mat)))

(defn ^DoubleMatrix
  info-matrix
  "returns the (symmetric) information matrix at the current iteration
  of the scoring algorithm (specifically the Newton-Raphson method.

  Example:
    (def beta (to-double-rowmat [0 0]))
    (def feat (to-double-matrix [[1 2] [4 5]]))
    (info-matrix beta feat) => #<DoubleMatrix [4.25, 5.5; 5.5, 7.25]>

  Reference:
    http://en.wikipedia.org/wiki/Scoring_algorithm
    http://en.wikipedia.org/wiki/Fisher_information"
  [beta-row feature-mat]
  {:pre [(= (.columns beta-row) (.columns feature-mat))]}
  (let [prob-row (probability-calc beta-row feature-mat)
        transformed-row (bernoulli-var prob-row)]
    (.mmul (.muliRowVector (.transpose feature-mat) transformed-row)
           feature-mat)))

(defn ^DoubleMatrix
  beta-increment
  "returns a DoubleMatrix row vector to increment the parameter
  vector, based on the information matrix.  The increment does not
  include a correction for the addition of a ridge factor.

  Example:
    (def beta (to-double-rowmat [0 0]))
    (def feat (to-double-matrix [[1 2] [4 5]]))
    (def label (to-double-rowmat [1 0]))
    (def score (score-seq beta label feat))
    (def info (info-matrix beta feat))
    (beta-increment info score 1e-2)
     => #<DoubleMatrix [-3.8961038961039076; 2.7449822904368464]>

  NOTE: cannot refactor DoubleMatrix/eye since it changes in memory,
  i.e., jBLAS objects don't behave like clojure data structures"
  [info-mat scores rdg-cons]
  (let [num-features (.rows info-mat)
        rdg-mat (.muli (DoubleMatrix/eye (int num-features)) (double rdg-cons))
        info-adj (.addi info-mat rdg-mat)]
    (.mmul (Solve/solve info-adj (DoubleMatrix/eye (int num-features)))
           scores)))

(defn ^DoubleMatrix
  ridge-correction
  "returns a vector of corrections to the beta-update, given the
  current iteration of the information matrix and score sequence.

  NOTES: The vector a is analogous to f(rdg_cons) = a*rdg_cons + b,
  and we are interested in the correction factor that is calculated as
  follows: Let v = rdg_cons.  Then we originally assume that f(0) =
  f(v) [equivalent to no added constant along the diagnol], which may
  not be correct.  A better approximation is f(0) - b, where b = f(v)
  - a*v.  Then, correction = f(0) - b = f(v) - f(v) + a*v = a*v."
  [info-mat scores rdg-cons]
  (let [stepsize (/ rdg-cons 10)
        upperbound (+ rdg-cons stepsize)
        lowerbound (- rdg-cons stepsize)
        f_upper (beta-increment info-mat scores upperbound)
        f_lower (beta-increment info-mat scores lowerbound)]
    (.divi (.muli (.rsubi f_upper f_lower) (double rdg-cons))
           (double (* 2 stepsize)))))

(defn ^DoubleMatrix
  beta-update
  "returns a beta update, including the ridge correction"
  [beta-row label-row feature-mat rdg-cons]
  (let [scores (score-seq beta-row label-row feature-mat)
        info-mat (info-matrix beta-row feature-mat)]
    (.addi (beta-increment info-mat scores rdg-cons)
           (ridge-correction info-mat scores rdg-cons))))

(defn ^DoubleMatrix
  initial-beta
  "returns a vector of intial weights; this can be changed to reflect
  the method of moments estimator as a starting point, but for now it
  is fine to assume no knowledge about the betas.  The probabilities
  start a 0.5 and are pushed to the 0 and 1 extremes as the likelihood
  function is maximized."
  [feature-mat]
  (let [n (.columns feature-mat)]
    (to-double-rowmat (repeat n 0))))

(defn logistic-beta-vector
  "return the estimated parameter vector (persistent vector); which is
  used, in turn, to calculate the estimated probability of the binary
  label.  The beta vector is based on the training labels and
  associated features collected in the feature matrix.  The output can
  be applied to new features using `probability-calc` in this
  namespace.

  Example:
    (def feat (to-double-matrix [[1 2] [4 5]]))
    (def label (to-double-rowmat [1 0]))
    (logistic-beta-vector label feat 1e-8 1e-6 250)
       => [-49.06118492894668 34.891861487354284]

  NOTES: the initial beta-diff value is an arbitrarily large value."
  [label-row feature-mat rdg-cons converge-threshold max-iter]
  (let [beta-init (initial-beta feature-mat)]
    (loop [beta beta-init
           iter max-iter
           beta-diff 100]
      (if (or (zero? iter)
              (< beta-diff converge-threshold))
        (vec (.toArray beta))
        (let [update (beta-update beta label-row feature-mat rdg-cons)
              beta-new (.addRowVector beta update)
              diff (.distance2 beta beta-new)]
          (recur
           beta-new
           (dec iter)
           diff))))))
