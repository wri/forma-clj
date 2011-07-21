(ns forma.hadoop.jobs.forma-test
  (:use forma.hadoop.jobs.forma
        cascalog.api
        [midje sweet cascalog])
  (:require [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [cascalog.ops :as c])
  (:import [forma.schema FormaNeighborValue]))

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

;; FORMA, broken down into pieces. We're going to have sixteen sample
;; timeseries, to test the business with the neighbors.

(def dynamic-tuples [["1000" "32" 13 9 0 0 370 (io/int-struct [3 2 1]) (io/int-struct [3 2 1]) (io/int-struct [3 2 1])]
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

(def fire-tuples [["1000" "32" 13 9 0 0 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 1 0 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 2 0 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 3 0 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 1 1 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 2 1 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 3 1 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 2 2 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 0 3 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]
                  ["1000" "32" 13 9 3 3 (io/fire-series 370 [(io/fire-tuple 1 1 1 1) (io/fire-tuple 0 1 1 1) (io/fire-tuple 3 2 1 1)])]])

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
                  ["1000" "32" 13 9 3 3 372  forma-1]
                  ]))

(let [est-map  {:est-start "2005-12-01"
                :est-end "2011-04-01"
                :t-res "32"
                :neighbors 1
                :window-dims [600 600]
                :vcf-limit 25
                :long-block 15
                :window 5}]
  (future-fact?- "TODO: Modify forma to handle new fire-series, etc,
   then come back to this."
                 outer-src (forma-tap est-map :n-src :r-src :v-src :f-src)
                 (provided
                   (dynamic-tap est-map (dynamic-filter 25 :n-src :r-src :v-src)) => dynamic-tuples
                   (fire-tap est-map :f-src) => fire-tuples)))

(def country-src
  (vec (for [sample (range 4)
             line (range 4)]
         ["1000" 13 9 sample line 1])))

;; (let [est-map {:neighbors 1 :window-dims [4 4]}]
;;   (test?- [[2]] (forma-query est-map :ndvi-src :rain-src
;;                 :vcf-src :country-src :fire-src) => true
;;     (provided
;;       (static-tap :country-src) => country-src
;;       (forma-tap est-map :ndvi-src :rain-src :vcf-src :fire-src) =>
;;       (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
;;           (outer-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val))))

;; (def ndvi-src [["ndvi" "1000" "32" 13 9 0 0 370 372 (io/int-struct [3 2 1])]])
;; (def rain-src [["ndvi" "1000" "32" 13 9 0 0 370 372 (io/int-struct [3 2 1])]])
;; (def fire-src [["ndvi" "1000" "32" 13 9 1199 866 (io/fire-series 370 372 [(io/fire-tuple 0 0 0 0)
;;                                                                           (io/fire-tuple 1 2 1 0)
;;                                                                           (io/fire-tuple 1 1 1 1)])]])
;;
;; (def new-fire-src
;;   (let [est-map {:est-start "2000-11-01" :est-end "2001-01-01" :t-res "32"}]
;;     (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?fire-series]
;;         (fire-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?f-start _ ?f-series)
;;         (adjust-fires est-map ?f-start ?f-series :> ?start ?fire-series))))

;; (def dynamic-src
;;   (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi-series ?precl-series]
;;       (ndvi-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?n-start _ ?n-series)
;;       (rain-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?r-start _ ?r-series)
;;       (adjust ?r-start ?r-series ?n-start ?n-series :> ?start ?precl-series ?ndvi-series)))

;; (defn null-test []
;;   (?<- (stdout)
;;        [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
;;        (dynamic-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?n-series ?p-series)
;;        (new-fire-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start !!f-series)
;;        (forma-schema !!f-series ?n-series ?p-series ?p-series :> ?forma-series)
;;        (p/struct-index ?start ?forma-series :> ?period ?forma-val)))

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


;; Test for the whole damned project!

;; (defn -main
;;   [out-path]
;;   (?- (io/forma-textline out-path "%s/%s/%s/%s/")
;;       (forma-query {:est-start "2005-12-01"
;;                     :est-end "2011-04-01"
;;                     :t-res "32"
;;                     :neighbors 1
;;                     :window-dims [4 4]
;;                     :vcf-limit 25
;;                     :long-block 15
;;                     :window 5}
;;                    [["ndvi" "1000" "32" 27 8 731 159 361 495 (io/int-struct [-3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000, -3000])]]
;;                    [["precl" "1000" "32" 27 8 731 159 360 502 (io/double-struct [171.0, 204.0, 102.0, 36.0, 46.0, 36.0, 4.0, 22.0, 23.0, 22.0, 57.0, 51.0, 144.0, 161.0, 111.0, 74.0, 50.0, 60.0, 10.0, 2.0, 2.0, 78.0, 62.0, 75.0, 204.0, 176.0, 101.0, 44.0, 42.0, 8.0, 3.0, 4.0, 1.0, 7.0, 44.0, 53.0, 201.0, 236.0, 110.0, 44.0, 44.0, 6.0, 3.0, 4.0, 9.0, 59.0, 50.0, 111.0, 177.0, 180.0, 112.0, 51.0, 59.0, 24.0, 13.0, 1.0, 12.0, 7.0, 65.0, 105.0, 137.0, 141.0, 116.0, 45.0, 35.0, 65.0, 19.0, 18.0, 25.0, 45.0, 51.0, 129.0, 194.0, 208.0, 90.0, 61.0, 55.0, 24.0, 2.0, 2.0, 1.0, 5.0, 26.0, 102.0, 114.0, 202.0, 74.0, 67.0, 42.0, 37.0, 10.0, 14.0, 5.0, 28.0, 61.0, 156.0, 162.0, 228.0, 100.0, 38.0, 19.0, 20.0, 3.0, 26.0, 12.0, 40.0, 66.0, 134.0, 202.0, 201.0, 52.0, 67.0, 60.0, 30.0, 11.0, 5.0, 8.0, 20.0, 48.0, 90.0, 206.0, 182.0, 125.0, 78.0, 92.0, 78.0, 37.0, 61.0, 73.0, 58.0, 60.0, 112.0, 133.0, 127.0, 106.0, 88.0, -999.0, -999.0, -999.0, -999.0, -999.0, -999.0, -999.0])]]
;;                    [["gadm" "1000" "32" "027008" 7 (io/int-struct (repeat 24000 130))]]
;;                    [["vcf" "1000" "32" "027008" 7 (io/int-struct (repeat 24000 130))]]
;;                    [["fire" "1000" "32" 27 8 731 159 360 502 (io/to-struct (repeat 126 (io/fire-tuple 1 1 1 1)))]])))

;; TODO: The above won't work anymore, since we can't just convert
;; fire-tuples to structs.

;; Test that outputs unique counts of various timeseries.
;;
;; (defn dynamic-test
;;   [est-map ndvi-src rain-src]
;;   (<- [?mod-h ?mod-v ?sample ?line
;;        ?n-start ?n-end ?r-start ?r-end ?start ?precl-count ?ndvi-count]
;;       (ndvi-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?n-start ?n-end ?n-series)
;;       (rain-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?r-start ?r-end ?r-series)
;;       (adjust ?r-start ?r-series ?n-start ?n-series :> ?start ?precl-series ?ndvi-series)
;;       (io/count-vals ?precl-series :> ?precl-count)
;;       (io/count-vals ?ndvi-series :> ?ndvi-count)))
