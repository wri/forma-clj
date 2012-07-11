;; This namespace tests the numerical properties of the logistic
;; classifier.

(ns forma.classify.logistic-test
  (:use [forma.classify.logistic] :reload)
  (:use [midje sweet]
        [cascalog.api]
        [clojure-csv.core])
  (:import [org.jblas FloatMatrix MatrixFunctions DoubleMatrix])
  (:require [forma.testing :as t]))

(defn csv->seq
  "Returns a lazy sequence of the rows in the CSV file located at the
  supplied path."
  [file-path]
  (map
   (partial map #(Float/parseFloat %))
   (butlast (parse-csv
             (slurp file-path)))))

(defn test-beta
  "Returns the coefficient vector for the supplied data sets.  The
  rows in each data set correspond to the labels and features to a
  specific pixel, so that the first row in each data set indicates the
  labels and features of the same pixel.  features in the first row of
  the feature data set."
  [label-path feature-path]
  (let [y (to-double-rowmat
           (apply concat (csv->seq label-path)))
        X (to-double-matrix
           (map (partial cons 1) (csv->seq feature-path)))]
    (logistic-beta-vector y X 1e-8 1e-10 6)))

(facts
  "Test the numerical outcomes of running the logistic classifier on a
  relatively small dataset extracted from Malaysia (features and
  labels for 1,000 pixels)."
  (let [label-path   (t/dev-path "/testdata/mys-label.csv")
        feature-path (t/dev-path "/testdata/mys-feature.csv")
        beta         (test-beta label-path feature-path)]
    (vector? beta) => true
    (last beta)    => (roughly -15.1054)
    (first beta)   => (roughly -2.41557)))

(fact
  "Test a basic property of the logistic probability function, namely
  that a linear combination that results in zero will yield a
  probability of 0.5; and that the result is an instance of a double
  row matrix"
  (logistic-prob (to-double-rowmat [1 2 3])
                 (to-double-rowmat [0 0 0])) => (to-double-rowmat [0.5]))
