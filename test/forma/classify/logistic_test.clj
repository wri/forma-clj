(ns forma.classify.logistic-test
  (:use [forma.classify.logistic] :reload)
  (:use [midje sweet cascalog]
        [clojure-csv.core]
        [cascalog.api])
  (:import [org.jblas FloatMatrix])
  (:require [incanter.core :as i]
            [forma.testing :as t]))

(defn read-mys-csv
  "returns a properly adjusted list of the malaysia test data."
  [file-name]
  (map
   (partial map #(Float/parseFloat %))
   (butlast (parse-csv
             (slurp file-name)))))

(def label-path (t/dev-path "/testdata/mys-label.csv"))
(def feature-path (t/dev-path "/testdata/mys-feature.csv"))

(def y (apply concat (read-mys-csv label-path)))

(def X (map (partial cons 1)
            (read-mys-csv feature-path)))

(def beta-init
  "define the initial parameter vector as a sequence of 0s, i.e., no
information on how each variable is weighted."
  (repeat (count (first X)) 0))

(facts
 "log-likelihood of a particular, binary label will always be the same
with a initialized parameter vector of 0's"
 (logistic-prob beta-init (first X)) => 0.5
 (log-likelihood beta-init (first y) (first X)) => -0.6931471805599453
 (total-log-likelihood beta-init y X) => -693.1471805599322)

(facts
 "logistic routine should return a vector of coefficients, with the
first and last specified, as below."
 (let [label-seq   y
       feature-mat X
       beta-output (logistic-beta-vector label-seq feature-mat 1e-8 1e-8 10)]
   (first beta-output) => -2.416103637233374
   (last beta-output)  => -26.652096814499775))

(def new-prob (estimated-probabilities y X X))

(defn make-binary
  [threshold val]
  (if (< val threshold) 0 1))

(def alerts (map (partial make-binary 0.5) new-prob))

(defn false-pos
  [actual estimated]
  (and (== 0 actual) (== 1 estimated)))

(fact
 "check false positives; test calculated probabilities against labels"
 (count (filter true?
                (map false-pos y alerts))) => 15)

(def sample-logist (vec (map (comp vec (partial take 5) flatten vector) y X)))

(defn check-labels []
  (<- [?label] [sample-logist ?label ?v1 ?v2 ?v3 ?v4]))

(fact?- [[0.0]
         [1.0]] (check-labels))

;; Start to test the classifier as a cascalog query

(defn create-sample-tap
  [n]
  (let [ecoid (map vector (repeat n "eco1"))
        obs (map conj ecoid (map vector y) (map vec X)) ]
    (vec (take n obs))))

(defbufferop get-probabilities
  [tuples]
  (let [y (vec (flatten (map first tuples)))
        X (vec (map second tuples))]
    (estimated-probabilities y X X)))

(defn show-logistic-results
  [n]
  (let [src (create-sample-tap n)]
    (?<- (stdout)
         [?eco ?prob]
         (src ?eco ?y ?X)
         (get-probabilities ?y ?X :> ?prob))))
