(ns forma.classify.logistic
  (:use [forma.utils]
        [forma.schema :only (unpack-neighbor-val)]
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

(defn
  ^DoubleMatrix
  logistic-fn
  [x]
  (let [exp-x (MatrixFunctions/exp x)
        one (DoubleMatrix/ones 1)]
    (.divi exp-x (.add exp-x one))))

(defn ^DoubleMatrix
  to-double-matrix
  "returns a DoubleMatrix instance for use with jBLAS functions"
  [mat]
  (DoubleMatrix.
   (into-array (map double-array mat))))

(defn ^DoubleMatrix
  to-double-rowmat
  [coll]
  (to-double-matrix [(vec coll)]))

(defn ^DoubleMatrix
  copy-row
  [row]
  (.copy (to-double-matrix [[]]) row))

(defn ^DoubleMatrix
  logistic-prob
  ".dot returns a double, which needs to be converted to a
  DoubleMatrix for logistic-fn"
  [beta-mat features-mat]
  (logistic-fn
   (to-double-rowmat [(.dot beta-mat features-mat)])))

;; TODO: convert the functions that are useful but not actually used
;; in estimation, like `log-likelihood` and `total-log-likelihood`.

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
  [beta feature-mat]
  (let [prob-row (.mmul beta (.transpose feature-mat))]
    (logistic-fn prob-row)))

(defn ^DoubleMatrix
  score-seq
  "returns the score for each parameter "
  [beta labels feature-mat]
    (let [prob-seq (probability-calc beta feature-mat)]
    (.mmul (.transpose feature-mat)
           (.transpose (.sub labels prob-seq)))))

(defn ^DoubleMatrix
  mult-fn
  [x]
  (let [ones (DoubleMatrix/ones (.rows x) (.columns x))
        new-mat (.add ones (.neg x))]
    (.mul x new-mat)))

(defn ^DoubleMatrix
  info-matrix
  [beta-row feature-mat]
  {:pre [(= (.columns beta-row) (.columns feature-mat))]}
  (let [prob-row (probability-calc beta-row feature-mat)
        transformed-row (mult-fn prob-row)]
    (.mmul (.muliRowVector (.transpose feature-mat) transformed-row)
           feature-mat)))

(defn ^DoubleMatrix
  beta-increment
  "increment beta without ridge correction; NOTE: cannot refactor
  DoubleMatrix/eye since it changes in memory, i.e., jBLAS objects
  don't behave like clojure data structures"
  [info-mat scores rdg-cons]
  (let [num-features (.rows info-mat)
        rdg-mat (.muli (DoubleMatrix/eye (int num-features)) (double rdg-cons))
        info-adj (.addi info-mat rdg-mat)]
    (.mmul (Solve/solve info-adj (DoubleMatrix/eye (int num-features)))
           scores)))

(defn ^DoubleMatrix
  ridge-correction
  "returns a vector of corrections to the beta-update, given the
  current iteration of the information matrix and score sequence. The
  vector a is analogous to f(rdg_cons) = a*rdg_cons + b, and we are
  interested in the correction factor that is calculated as follows:
  Let v = rdg_cons.  Then we originally assume that f(0) = f(v)
  [equivalent to no added constant along the diagnol], which may not
  be correct.  A better approximation is f(0) - b, where b = f(v) -
  a*v.  Then, correction = f(0) - b = f(v) - f(v) + a*v = a*v."
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
  "full beta update, including correction"
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
  "return the estimated parameter vector; which is used, in turn, to
  calculate the estimated probability of the binary label; the initial
  beta-diff value is an arbitrarily large value."
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

(defn unpack-neighbors
  "Returns a vector containing the fields of a forma-neighbor-value."
  [neighbor-val]
  (map (partial get neighbor-val)
       [:fire-value :neighbor-count
        :avg-short-drop :min-short-drop
        :avg-long-drop :min-long-drop
        :avg-t-stat :min-t-stat]))

(defn unpack-fire [fire]
  (map (partial get fire)
       [:temp-330 :conf-50 :both-preds :count]))

(defn unpack-feature-vec [val neighbor-val]
  (let [[fire short _ long t-stat] val
        fire-seq (unpack-fire fire)
        [fire-neighbors & more] (unpack-neighbors neighbor-val)
        fire-neighbor (unpack-fire fire-neighbors)]
    (into [] (concat fire-seq [short long t-stat] fire-neighbor more))))


(defn make-binary
  [x]
  (if (zero? x) 0 1))

(defbufferop [logistic-beta-wrap [r c m]]
  "returns a vector of parameter coefficients.  note that this is
  where the intercept is added (to the front of each stacked vector in
  the feature matrix

  TODO: The intercept is included in the feature vector for now, as a
  kludge when we removed the hansen statistic.  When we include the
  hansen stat, we will have to replace the feature-mat binding below
  with a line that tacks on a 1 to each feature vector.
  "
  [tuples]
  (let [label-seq    (map (comp make-binary first) tuples) 
        val-mat      (map second tuples) 
        neighbor-mat (map last tuples)
        feature-mat  (map unpack-feature-vec val-mat neighbor-mat)]
    [[(logistic-beta-vector
       (to-double-rowmat label-seq)
       (to-double-matrix feature-mat)
       r c m)]]))

(defn logistic-prob-wrap
  [beta-vec val neighbor-val]
  (let [beta-mat (to-double-rowmat beta-vec)
        features-mat (to-double-rowmat (unpack-feature-vec val neighbor-val))]
    (flatten (vec (.toArray (logistic-prob beta-mat features-mat))))))

(defbufferop mk-timeseries
  [tuples]
  [[(map second (sort-by first tuples))]])

(defn mk-key
  [k]
  (keyword (str k))) 

(defn make-dict
  [v]
  {(mk-key (second v))
   (last v)})

(defn beta-dict
  "create dictionary of beta vectors"
  [beta-src]
  (let [src (name-vars beta-src ["?s-res" "?eco" "?beta"])
        beta-vec (first (??- (c/first-n src 500)))]
    (apply merge-with identity
           (map make-dict beta-vec))))

