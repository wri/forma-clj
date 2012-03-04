(ns forma.classify.logistic-test
  (:use [forma.classify.logistic] :reload)
  (:use [midje sweet cascalog]
        [clojure-csv.core]
        [cascalog.api]
        [clojure.test]
        [midje.cascalog])
  (:import [org.jblas FloatMatrix])
  (:require [incanter.core :as i]
            [forma.testing :as t]
            [forma.date-time :as date]))

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

(fact
 (logistic-prob (to-double-rowmat [1 2 3])
                (to-double-rowmat [0.5 0.5 0.5])) => (roughly 0.9525))

(facts
 (let [ym (to-double-rowmat y)
       Xm (to-double-matrix X)
       x  (to-double-rowmat (first X))
       beta (logistic-beta-vector ym Xm 1e-8 1e-10 6)]
   (vector? beta) => true
   (last beta)    => (roughly -8.4745)
   (first beta)   => (roughly -1.7796)
   (logistic-prob (initial-beta Xm) x) => 0.5))




;; (def new-prob (estimated-probabilities y X X))

;; (defn thresh-make-binary
;;   [threshold val]
;;   (if (< val threshold) 0 1))

;; (def alerts (map (partial thresh-make-binary 0.5) new-prob))

;; (defn eco-generator [n]
;;   (map vector (map #(if (odd? %) "eco1" "eco2") (range n))))

;; (defn create-sample-tap
;;   "creates a sample tap with `n` observations, with the ecoregion
;;   string, the label, and the feature vector"
;;   [n]
;;   (let [ecoid (eco-generator n)
;;         obs (map conj ecoid (map vector (range n)) (map vector y) (map vec X))]
;;     (vec (take n obs))))

;; (defbufferop [logistic-beta-wrap-test [r c m]]
;;   "returns a vector of coefficients that is accepted within the
;;   framework of cascalog."
;;   [tuples]
;;   (let [label-seq (flatten (map first tuples))
;;         feature-mat (map second tuples)]
;;     [[(logistic-beta-vector label-seq feature-mat r c m)]]))

;; (deftest group-probabilities-test
;;   "test that the classifier returns two alerts in this example for a
;;   given ecoregion, constructed \"eco1\".  This method first calculates
;;   a separate coefficient vector for each ecoregion, and then applies
;;   the appropriate coefficient vector to each pixel.  Note that
;;   `?feat-training` is the feature set over the training period,
;;   whereas `?feat-update` is the feature set through the updated
;;   interval.  Here, for this example, the two feature sets are
;;   identical, which means that the probability output is defined as the
;;   probability of deforestation *during* the training period. This
;;   seems a little round-about, but this workflow will reduce redundant
;;   calculation as we estimate probabilities for more than one time
;;   period (or any other time period than the training period)

;;   FOR THIS EXAMPLE: return the probabilities for the alerts identified
;;   for the sample ecoregion, eco1, as well as the arbitrary, unique
;;   pixel identifiers

;;   NOTE!!!  This example perfectly identifies the two alerts in eco1"
;;   (fact
;;    (let [src (create-sample-tap 100)
;;          beta-gen (<- [?eco ?beta]
;;                       (src ?eco ?pixel-id ?labels ?feat-training)
;;                       (logistic-beta-wrap-test [1e-8 1e-6 250] ?labels ?feat-training
;;                                           :> ?beta))
;;          alerts-query (<- [?eco ?pixel-id ?prob]
;;                           (src ?eco ?pixel-id ?labels ?feat-update)
;;                           (= ?eco "eco1")
;;                           (beta-gen ?eco ?beta)
;;                           (logistic-prob ?beta ?feat-update :> ?prob)
;;                           (> ?prob 0.5))]
;;      alerts-query => (produces [["eco1" [31] 0.9999999999963644]
;;                                 ["eco1" [49] 0.9999999999765928]]))))


