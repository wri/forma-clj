(ns forma.classify.logistic-test
  (:use [forma.classify.logistic] :reload)
  (:use [midje sweet cascalog]
        [clojure-csv.core]
        [cascalog.api]
        [clojure.test]
        [midje.cascalog])
  (:import [org.jblas FloatMatrix MatrixFunctions DoubleMatrix])
  (:require [incanter.core :as i]
            [forma.testing :as t]
            [forma.date-time :as date]
            [cascalog.ops :as c]))

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


(def y-mat
  (to-double-rowmat y))
(def X-mat
  (to-double-matrix X))

(fact
  (probability-calc beta-init X-mat) => [5])

(fact
  (let [e (DoubleMatrix/eye 2)]
    (mult-fn e) => [5]))

(fact
  (info-matrix beta-init X-mat) => [5])

(fact
  "Make sure mult-fn is working for tiny matrix"
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

(comment
  (fact
  (let [r 1e-8
        c 1e-6
        m 500
        sres "500"
        eco 40103
        src (hfs-seqfile (format "/Users/robin/Downloads/eco-%d-small" eco))]
    (??<- [?beta]
          (src ?hansen ?val ?n-val)
          (logistic-beta-wrap [r c m] ?hansen ?val ?n-val :> ?beta)) => (produces [[5]])))
    (let [path "/Users/robin/Downloads/eco-40103-small"
      src (-> (hfs-seqfile path)
              (name-vars ["?word" "?count" "a"]))]
    (c/first-n src 10))

  (let [src (hfs-seqfile "/Users/robin/Downloads/eco-40103-small")])

 (c/first-n 10
             (<- (hfs-textline "/Users/robin/delete/eco-40103" :sinkmode :replace)
                 [?hansen ?val ?n-val]
                 (src ?hansen ?val ?n-val))))