(ns forma.classify.logistic-test
  (:use [forma.classify.logistic] :reload)
  (:use [midje sweet cascalog]
        [clojure-csv.core]
        [cascalog.api]
        [clojure.test]
        [midje.cascalog])
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


(defn eco-generator [n]
  (map vector (map #(if (odd? %) "eco1" "eco2") (range n))))

(defn create-sample-tap
  "creates a sample tap with `n` observations, with the ecoregion
  string, the label, and the feature vector"
  [n]
  (let [ecoid (eco-generator n)
        obs (map conj ecoid (map vector (range n)) (map vector y) (map vec X))]
    (vec (take n obs))))

(defbufferop logistic-beta-wrap
  "returns a vector of coefficients that is accepted within the
  framework of cascalog."
  [tuples]
  (let [label-seq (flatten (map first tuples))
        feature-mat (map second tuples)]
    [[(logistic-beta-vector label-seq feature-mat 1e-8 1e-6 250)]]))

(deftest group-probabilities-test
  "test that the classifier returns two alerts in this example for a
  given ecoregion, constructed \"eco1\".  This method first calculates
  a separate coefficient vector for each ecoregion, and then applies
  the appropriate coefficient vector to each pixel.  Note that
  `?feat-training` is the feature set over the training period,
  whereas `?feat-update` is the feature set through the updated
  interval.  Here, for this example, the two feature sets are
  identical, which means that the probability output is defined as the
  probability of deforestation *during* the training period. This
  seems a little round-about, but this workflow will reduce redundant
  calculation as we estimate probabilities for more than one time
  period (or any other time period than the training period)

  FOR THIS EXAMPLE: return the probabilities for the alerts identified
  for the sample ecoregion, eco1, as well as the arbitrary, unique
  pixel identifiers

  NOTE!!!  This example perfectly identifies the two alerts in eco1"
  (fact
   (let [src (create-sample-tap 100)
         beta-gen (<- [?eco ?beta]
                      (src ?eco ?pixel-id ?labels ?feat-training)
                      (logistic-beta-wrap ?labels ?feat-training :> ?beta))
         alerts-query (<- [?eco ?pixel-id ?prob]
                          (src ?eco ?pixel-id ?labels ?feat-update)
                          (= ?eco "eco1")
                          (beta-gen ?eco ?beta)
                          (logistic-prob ?beta ?feat-update :> ?prob)
                          (> ?prob 0.5))]
     alerts-query => (produces [["eco1" [31] 0.9999999999963644]
                                ["eco1" [49] 0.9999999999765928]]))))
