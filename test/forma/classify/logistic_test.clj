(ns forma.classify.logistic-test
  (:use [forma.classify.logistic] :reload)
  (:use [midje sweet cascalog]
        [clojure-csv.core]
        [cascalog.api]
        [midje.cascalog]
        [clojure-csv.core])
  (:import [org.jblas FloatMatrix MatrixFunctions DoubleMatrix])
  (:require [incanter.core :as i]
            [forma.testing :as t]
            [forma.date-time :as date]
            [cascalog.ops :as c]))

(defn read-mys-csv
  "returns a properly adjusted list of the malaysia test data."
  [file-name]
  (map
   (partial map #(Float/parseFloat %))
   (butlast (parse-csv
             (slurp file-name)))))

(def label-path (t/dev-path "/testdata/mys-label.csv"))
(def feature-path (t/dev-path "/testdata/mys-feature.csv"))

(def y
  "returns a list of binary labels for training."
  (apply concat (read-mys-csv label-path)))

(def X
  "returns a clojure object of the feature vectors, with a constant
  prepended."
  (map (partial cons 1) (read-mys-csv feature-path)))

(fact
  "a linear combination that results in zero will yield a probability of 0.5"
  (logistic-prob (to-double-rowmat [1 2 3])
                 (to-double-rowmat [0 0 0])) => (to-double-rowmat [0.5]))

(def y-mat (to-double-rowmat y))
(def X-mat (to-double-matrix X))

(facts
 (let [beta (logistic-beta-vector y-mat X-mat 1e-8 1e-10 6)]
   (vector? beta) => true
   (last beta)    => (roughly -15.1054)
   (first beta)   => (roughly -2.4156)))

(fact
"TODO: (robin) what is this test supposed to do?  More docs. Make
sure mult-fn is working for tiny matrix"
  (let [mat
        (let [init-mat (.transpose (DoubleMatrix/zeros 4)) 
              to-insert (double 2)]
          (.put init-mat 0 to-insert)
          (.put init-mat 3 to-insert))
        out-mat
        (let [init-mat (.transpose (DoubleMatrix/zeros 4)) 
              to-insert (double -2)]
          (.put init-mat 0 to-insert)
          (.put init-mat 3 to-insert))]
    (mult-fn mat) => out-mat))

;; USEFUL FOR BENCHMARK

(defn multiplier
  [n]
  ((partial * 1000) n))

(defn mk-x
  [n]
  (DoubleMatrix/rand (multiplier n) 20))

(defn mk-y
  [n]
  (to-double-rowmat (repeatedly (multiplier n) #(rand-int 2))))

(defn run-logistic-beta-vector
  "Run logistic-beta-vector on dataset of size n * 1000, with up to specified iterations"
  [n iterations]
  (let [big-X (mk-x n)
        big-y (mk-y n)]
    (prn (.rows big-X) (.columns big-X))
    (logistic-beta-vector big-y big-X 1e-8 1e-10 iterations)))

;; TIME required to calculate logistic betas for 1 million obs, 10 iterations
;; (time (run-logistic-beta-vector 1000 10))
;; "Elapsed time: 5014.590245 msecs"
