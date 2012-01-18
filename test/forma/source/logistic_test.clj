(ns forma.source.logistic-test
  (:use [forma.source.logistic] :reload)
  (:use [midje sweet cascalog]
        [clojure-csv.core])
  (:import [org.jblas FloatMatrix])
  (:require [incanter.core :as i]
            [clojure.java.io :as io]))

(defn read-csv
  [file-name]
  (map
   (partial map #(Float/parseFloat %))
   (parse-csv
    (slurp file-name))))

(defn test-file
  [file-name]
  (io/input-stream (io/resource file-name)))

(def y (apply concat
              (read-csv (test-file "testdata/mys-label-data.csv"))))

(def X (map (partial cons 1)
            (read-csv (test-file "testdata/mys-feature-data.csv"))))

;; (def y (apply
;;         concat
;;         (take 1000 (read-csv "/Users/danhammer/Desktop/myslab.csv"))))

;; (def X (take 1000
;;              (map (partial cons 1)
;;                   (read-csv "/Users/danhammer/Desktop/mys.csv"))))

(def beta (repeat
           (count (first X))
           0))

(facts
 (let [label-seq   y
       feature-mat X
       beta-output (logistic-beta-vector label-seq feature-mat 1e-8)]
   (first beta-output) => -2.416103637233374
   (last beta-output)  => -26.652096814499775))

(defn feature-vec
  [n]
  (for [x (range n)]
    (take 23 (repeatedly rand))))

(defn label-vec
  [n]
  (for [x (range n)] (if (> (rand) 0.5) 1 0)))

(def A [[14 9 3] [2 11 15] [0 12 17] [5 2 3]])
(def B [[12 25] [9 10] [8 5]])

(fact
 "test algebra for matrix multiplication used in logistic.clj namespace"
 (let [m1 (FloatMatrix. (into-array (map float-array A)))
       m2 (FloatMatrix. (into-array (map float-array B)))]
   (vec (.data (.rowSums (.mmul m1 m2)))))
 => [728.0 478.0 449.0 262.0])

(fact
 "no new information is added if beta is 0-vector; probability is 0.5"
 (let [xs (feature-vec 4)
       beta (repeat 23 0)]
   (logistic-prob beta (first xs))) => 0.5)

(fact (let [beta (repeat 1000 0)
            labels (label-vec 1000)
            features (feature-vec 1000)]
        (total-log-likelihood beta labels features)) => -693.1471805599322)

;; (:use [clojure-csv.core])
;; (def mys-data (let [file "/Users/danhammer/Desktop/testmys/allmys.txt"]
;;                 (map
;;                  (partial map #(Float/parseFloat %))
;;                  (parse-csv
;;                   (slurp file)))))

;; (def mys-labels (take 1000000 (map last mys-data)))
;; (def mys-features (take 1000000 (map butlast mys-data)))

;; (defn make-binary
;;   [coll]
;;   (map #(if (> % 0) 1 0) coll))

;; (def y (make-binary mys-labels))
;; (def X (map (partial cons 1) (take 235469 mys-features)))
;; (def beta (repeat 23 0))



