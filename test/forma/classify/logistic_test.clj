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
   (logistic-prob-wrap beta) => [0.5]))

(def y-mat
  (to-double-rowmat y))
(def X-mat
  (to-double-matrix X))

(def beta-init 1)

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

(def feature-vec
  [[0 0 0 0 -157.69368186874303 0.19138443637714708 0.04501147771152674 0 0 0 0 4 -153.08778642936596 -226.48974416017805 6.2492727447048075 -3.550102068394011 1.4331905477662108 -0.7384098147942525]])

(def beta-src
  ;; designed to match static-src eco-id
  [["500" 40102 [0.0 0.0 0.0 0.0 0.014124574304861895 -0.07149047035736451 -0.26117313338623815 0.0 0.0 0.0 0.0 -0.8754775060538595 0.014841138264409883 -0.028689426585205655 -0.03933755108463727 0.012033437671756119 0.05472598631539089 -0.5607842240019152]]])

(def beta-vec
  [0.0 0.0 0.0 0.0 0.014124574304861895 -0.07149047035736451 -0.26117313338623815 0.0 0.0 0.0 0.0 -0.8754775060538595 0.014841138264409883 -0.028689426585205655 -0.03933755108463727 0.012033437671756119 0.05472598631539089 -0.5607842240019152])

(def static-src
  ;; defined to match something on robin's computer
  [["500" 31 9 1480 583 -9999	57 40102 0]])

(fact
  (beta-dict beta-src) => {:40102 [0.0 0.0 0.0 0.0 0.014124574304861895 -0.07149047035736451 -0.26117313338623815 0.0 0.0 0.0 0.0 -0.8754775060538595 0.014841138264409883 -0.028689426585205655 -0.03933755108463727 0.012033437671756119 0.05472598631539089 -0.5607842240019152]})

(def my-val
  [["500" 31 9 1480 583 [{:temp-330 0, :conf-50 0, :both-preds 0, :count 0} -157.69368186874303 1 0.19138443637714708 0.04501147771152674]]])

(def my-neighbor-val
  [["500" 31 9 1480 583 [{:fire-value #forma.schema.FireValue{:temp-330 0, :conf-50 0, :both-preds 0, :count 0}, :neighbor-count 4, :avg-short-drop -153.08778642936596, :min-short-drop -226.48974416017805, :avg-param-break 1, :min-param-break 1, :avg-long-drop 6.2492727447048075, :min-long-drop -3.550102068394011, :avg-t-stat 1.4331905477662108, :min-t-stat -0.7384098147942525}]]])

(comment
  (fact
    (let [dynamic-src (hfs-seqfile "/Users/robin/Downloads/dynamic")]
      (??<- [?mod-h ?mod-v ?s ?l ?eco ?val ?neighbor-val]
            (beta-src ?s-res ?eco ?beta)
            (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco ?hansen)
            (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
            (logistic-prob-wrap ?beta ?val ?neighbor-val :> ?prob))) => (produces [0.5])))

(fact
  (let [dynamic-src (hfs-seqfile "/Users/robin/Downloads/dynamic")]
      (??<- [?prob]
            (beta-src ?s-res ?eco ?beta)
            (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco ?hansen)
            (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
            (logistic-prob-wrap ?beta ?val ?neighbor-val :> ?prob))) => [[0.5]])
;; (logistic-prob (to-double-matrix feature-vec) (to-double-rowmat beta-vec))