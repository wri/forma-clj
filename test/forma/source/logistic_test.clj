(ns forma.source.logistic-test
  (:use [forma.source.logistic] :reload)
  (:use [midje sweet cascalog])
  (:require [incanter.core :as i]))

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
 (matrix-mult A B) => [[273 455] [243 235] [244 205] [102 160]])

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



