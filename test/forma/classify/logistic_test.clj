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

;; TODO: Write cascalog.midje tests

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

