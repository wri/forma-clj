(ns forma.hadoop.jobs.run-forma-test
  (:use forma.hadoop.jobs.run-forma
        cascalog.api
        midje.sweet)
  (:require [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [cascalog.ops :as c])
  (:import [forma.schema FormaNeighborValue]))

(def neighbors [(io/forma-value nil 1 1 1)
                (io/forma-value nil 2 2 2)])

(fact
  "Tests that the combine neighbors function produces the proper
textual representation."
  (textify 1 1 1 1 (first neighbors) (combine-neighbors neighbors)) =>
  "1 1 1 1 0 0 0 0 1.0 1.0 1.0 0 0 0 0 1.5 1.0 1.5 1.0 1.5 1.0")

(def some-map
  {:est-start "2005-12-01"
   :est-end "2011-04-01"
   :t-res "32"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 15
   :window 5})

(defn run-dynamic
  "Test function that displays the output of the dynamic tap."
  []
  (?- (stdout)
      (-> (dynamic-tap some-map
                       (hfs-seqfile "/Users/sritchie/Desktop/ndviseries1000/")
                       (hfs-seqfile "/Users/sritchie/Desktop/ndviseries1000/"))
          (c/first-n 10))))

(fact
  "Checks that neighbors are being combined properly."
  (let [test-seq [(io/forma-value nil 1 1 1) (io/forma-value nil 2 2 2 )]]
    (combine-neighbors test-seq) => (FormaNeighborValue. (io/fire-tuple 0 0 0 0)
                                                         2
                                                         1.5 1.0
                                                         1.5 1.0
                                                         1.5 1.0)))

;; FORMA, broken down into pieces. We're going to have sixteen sample
;; timeseries, to test the business with the neighbors.

(def dynamic-src [["1000" "32" 13 9 0 0 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 1 0 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 2 0 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 3 0 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 0 1 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 1 1 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 2 1 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 3 1 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 0 2 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 1 2 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 2 2 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 3 2 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 0 3 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 1 3 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 2 3 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
                  ["1000" "32" 13 9 3 3 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]])

(def fire-src [["1000" "32" 13 9 0 0 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 1 0 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 2 0 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 3 0 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 1 1 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 2 1 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 3 1 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 2 2 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 0 3 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
               ["1000" "32" 13 9 3 3 370 (io/fire-series [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]])

(def outer-src (let [no-fire-3 (io/forma-value nil 3.0 3.0 3.0)
                     no-fire-2 (io/forma-value nil 2.0 2.0 2.0)
                     no-fire-1 (io/forma-value nil 1.0 1.0 1.0)
                     forma-3 (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
                     forma-2 (io/forma-value (io/fire-tuple 0 1 1 1) 2.0 2.0 2.0)
                     forma-1 (io/forma-value (io/fire-tuple 3 2 1 1) 1.0 1.0 1.0)]
                 [
                  ["1000" "32" 13 9 0 0 370  forma-3]
                  ["1000" "32" 13 9 0 0 371  forma-2]
                  ["1000" "32" 13 9 0 0 372  forma-1]
                  ["1000" "32" 13 9 1 0 370  forma-3]
                  ["1000" "32" 13 9 1 0 371  forma-2]
                  ["1000" "32" 13 9 1 0 372  forma-1]
                  ["1000" "32" 13 9 2 0 370  forma-3]
                  ["1000" "32" 13 9 2 0 371  forma-2]
                  ["1000" "32" 13 9 2 0 372  forma-1]
                  ["1000" "32" 13 9 3 0 370  forma-3]
                  ["1000" "32" 13 9 3 0 371  forma-2]
                  ["1000" "32" 13 9 3 0 372  forma-1]
                  ["1000" "32" 13 9 0 1 370  no-fire-3]
                  ["1000" "32" 13 9 0 1 371  no-fire-2]
                  ["1000" "32" 13 9 0 1 372  no-fire-1]
                  ["1000" "32" 13 9 1 1 370  forma-3]
                  ["1000" "32" 13 9 1 1 371  forma-2]
                  ["1000" "32" 13 9 1 1 372  forma-1]
                  ["1000" "32" 13 9 2 1 370  forma-3]
                  ["1000" "32" 13 9 2 1 371  forma-2]
                  ["1000" "32" 13 9 2 1 372  forma-1]
                  ["1000" "32" 13 9 3 1 370  forma-3]
                  ["1000" "32" 13 9 3 1 371  forma-2]
                  ["1000" "32" 13 9 3 1 372  forma-1]
                  ["1000" "32" 13 9 0 2 370  no-fire-3]
                  ["1000" "32" 13 9 0 2 371  no-fire-2]
                  ["1000" "32" 13 9 0 2 372  no-fire-1]
                  ["1000" "32" 13 9 1 2 370  no-fire-3]
                  ["1000" "32" 13 9 1 2 371  no-fire-2]
                  ["1000" "32" 13 9 1 2 372  no-fire-1]
                  ["1000" "32" 13 9 2 2 370  forma-3]
                  ["1000" "32" 13 9 2 2 371  forma-2]
                  ["1000" "32" 13 9 2 2 372  forma-1]
                  ["1000" "32" 13 9 3 2 370  no-fire-3]
                  ["1000" "32" 13 9 3 2 371  no-fire-2]
                  ["1000" "32" 13 9 3 2 372  no-fire-1]
                  ["1000" "32" 13 9 0 3 370  forma-3]
                  ["1000" "32" 13 9 0 3 371  forma-2]
                  ["1000" "32" 13 9 0 3 372  forma-1]
                  ["1000" "32" 13 9 1 3 370  no-fire-3]
                  ["1000" "32" 13 9 1 3 371  no-fire-2]
                  ["1000" "32" 13 9 1 3 372  no-fire-1]
                  ["1000" "32" 13 9 2 3 370  no-fire-3]
                  ["1000" "32" 13 9 2 3 371  no-fire-2]
                  ["1000" "32" 13 9 2 3 372  no-fire-1]
                  ["1000" "32" 13 9 3 3 370  forma-3]
                  ["1000" "32" 13 9 3 3 371  forma-2]
                  ["1000" "32" 13 9 3 3 372  forma-1]]))


(let [est-map  {:est-start "2005-12-01"
                :est-end "2011-04-01"
                :t-res "32"
                :neighbors 1
                :window-dims [600 600]
                :vcf-limit 25
                :long-block 15
                :window 5}]
  (fact (apply set (??- (forma-tap est-map :ndvi-src :rain-src :vcf-src :fire-src))) => (set outer-src)
    (provided (dynamic-tap est-map :ndvi-src :rain-src :vcf-src) => dynamic-src
              (fire-tap est-map :fire-src) => fire-src)))

(def country-src [["1000" 13 9 0 0 1]
                  ["1000" 13 9 1 0 1]
                  ["1000" 13 9 2 0 1]
                  ["1000" 13 9 3 0 1]
                  ["1000" 13 9 0 1 1]
                  ["1000" 13 9 1 1 1]
                  ["1000" 13 9 2 1 1]
                  ["1000" 13 9 3 1 1]
                  ["1000" 13 9 0 2 1]
                  ["1000" 13 9 1 2 1]
                  ["1000" 13 9 2 2 1]
                  ["1000" 13 9 3 2 1]
                  ["1000" 13 9 0 3 1]
                  ["1000" 13 9 1 3 1]
                  ["1000" 13 9 2 3 1]
                  ["1000" 13 9 3 3 1]])

;; TODO: Assign a proper return value to this test.
;;
;; (let [est-map {:neighbors 1 :window-dims [4 4]}]
;;   (fact (apply set (?- (stdout) (forma-query est-map :ndvi-src :rain-src :vcf-src :country-src :fire-src))) => 2
;;     (provided
;;       (static-tap :country-src) => country-src
;;       (forma-tap est-map :ndvi-src :rain-src :vcf-src :fire-src) =>
;;       (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
;;           (outer-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val)))))

(def ndvi-src [["ndvi" "1000" "32" 13 9 0 0 370 372 (io/int-struct [3 2 1])]])
(def rain-src [["ndvi" "1000" "32" 13 9 0 0 370 372 (io/int-struct [3 2 1])]])
(def fire-src [["ndvi" "1000" "32" 13 9 1199 866 370 372 (io/fire-series
                                                          [(io/fire-tuple 0 0 0 0)
                                                           (io/fire-tuple 1 2 1 0)
                                                           (io/fire-tuple 1 1 1 1)])]])

(def new-fire-src
  (let [est-map {:est-start "2000-11-01" :est-end "2001-01-01" :t-res "32"}]
    (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?fire-series]
        (fire-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?f-start _ ?f-series)
        (adjust-fires est-map ?f-start ?f-series :> ?start ?fire-series))))

(def dynamic-src
  (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi-series ?precl-series]
      (ndvi-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?n-start _ ?n-series)
      (rain-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?r-start _ ?r-series)
      (adjust ?r-start ?r-series ?n-start ?n-series :> ?start ?precl-series ?ndvi-series)))

(defn null-test []
  (?<- (stdout)
       [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
       (dynamic-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?n-series ?p-series)
       (new-fire-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start !!f-series)
       (forma-schema !!f-series ?n-series ?p-series ?p-series :> ?forma-series)
       (p/struct-index ?start ?forma-series :> ?period ?forma-val)))

;; Tests for the process-neighbors defmapcatop.
;;
;; (def tester-src [[[[(io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)]
;;                    [(io/forma-value (io/fire-tuple 0 0 0 0) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)]
;;                    [(io/forma-value (io/fire-tuple 0 0 0 0) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 0 0 0 0) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 0 0 0 0) 3.0 3.0 3.0)]
;;                    [(io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 0 0 0 0) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 0 0 0 0) 3.0 3.0 3.0)
;;                     (io/forma-value (io/fire-tuple 1 1 1 1) 3.0 3.0 3.0)]]]])

;; (?<- (stdout)
;;      [?window ?win-idx ?val ?neighbor-vals]
;;      (tester-src ?window)
;;      (process-neighbors [1] ?window :> ?win-idx ?val ?neighbor-vals))
