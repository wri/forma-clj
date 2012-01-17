(ns forma.source.logistic
  (:use [clojure-csv.core])
  (:require [incanter.core :as i]))

(def mys-data (let [file "/Users/danhammer/Desktop/testmys/allmys.txt"]
                (map
                 (partial map #(Float/parseFloat %))
                 (parse-csv
                  (slurp file)))))

(def mys-labels (take 1000000 (map last mys-data)))
(def mys-features (take 1000000 (map butlast mys-data)))

(defn make-binary
  [coll]
  (map #(if (> % 0) 1 0) coll))

(def y (make-binary mys-labels))
(def X (map (partial cons 1) (take 235469 mys-features)))
(def beta (repeat 23 0))

(defn feature-vec
  [n]
  (map (partial cons 1)
       (for [x (range n)]
         (take 22 (repeatedly rand)))))

(defn label-vec
  [n]
  (for [x (range n)] (if (> (rand) 0.5) 1 0)))

(defn scaled-vector
  [scalar coll]
  (map #(* scalar %) coll))

(defn logistic-fn
  [x]
  (let [exp-x (Math/exp x)]
    (/ exp-x (inc exp-x))))

(defn dot-product
  [x y]
  (reduce + (map * x y)))

(defn logistic-prob
  [beta-seq x]
  (logistic-fn (dot-product beta-seq x)))

(defn log-likelihood
  [beta-seq label x]
  (let [prob (logistic-prob beta-seq x)]
    (+ (* label (Math/log prob))
       (* (- 1 label) (Math/log (- 1 prob))))))

(defn total-log-likelihood
  "returns the total log likelihood for a group of pixels; input
  labels and features for the group of pixels, aligned correctly so
  that the first label and feature correspond to the first pixel."
  [beta-seq labels features]
  (reduce + (map (partial log-likelihood beta-seq) labels features)))

(defn probability-calc
  "returns a vector of probabilities for each observation"
  [beta-seq feature-seqs]
  (map (partial logistic-prob beta-seq)
       feature-seqs))

(defn score-seq
  "returns the scores for each parameter"
  [beta-seq labels features]
  (let [prob-seq (probability-calc beta-seq features)]
    (i/mmult (i/trans features) (map - labels prob-seq))))

(defn info-matrix
  "returns the information matrix for the logistic probability
  function"
  [beta-seq labels features]
  (let [mult-func (fn [x] (* x (- 1 x)))
        prob-seq  (->> (probability-calc beta-seq features)
                       (map mult-func))]
    (i/mmult (map scaled-vector prob-seq (i/trans features))
             features)))

(defn beta-update
  [beta-seq labels features rdg-cons]
  (let [num-features (count beta-seq)
        info-adj (i/plus
                  (info-matrix beta-seq labels features)
                  (i/diag (repeat num-features rdg-cons)))]
    (i/mmult
     (i/solve info-adj)
     (score-seq beta-seq labels features))))

(defn logistic-beta-vector 
  [labels features rdg-cons]
  (let [b (repeat 23 0)]
    (loop [beta b iter 9]
      (if (zero? iter)
        beta
        (recur (let [inc-beta (beta-update beta labels features rdg-cons)]
                 (map + beta inc-beta))
               (dec iter))))))

;; Ultimately, this is what we'd like to run:
;; (logistic-beta-vector (label-vec 100000) (feature-vec 100000) 0.00000001)

;; In the meantime, this is a problem ...
;; (def X-rand (feature-vec 100000))
;; (def y-rand (label-vec 100000))
;; (time (total-log-likelihood beta y-rand X-rand))
;; =>"Elapsed time: 3320.955 msecs"

;; (def X-rand (feature-vec 100000))
;; (def y-rand (label-vec 100000))
;; (time (total-log-likelihood beta y-rand X-rand))
;; ... takes way, way longer than 33200 msecs (10 x 3320 msecs)
