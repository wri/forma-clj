(ns forma.postprocess.select_test
  (:use [forma.postprocess.select] :reload)
  (:use [midje sweet cascalog]
        [cascalog.api]
        [clojure.test]
        [midje.cascalog]
        [forma.matrix.utils :only (transpose)]
        [forma.utils :only (positions)]
        [forma.date-time :only (period->datetime datetime->period)]
        [forma.trends.data :only (ndvi rain reli)]
        [forma.reproject :only (modis->latlon)])
  (:require [incanter.core :as i]
            [incanter.charts :as chart]
            [forma.testing :as t]
            [cascalog.ops :as c]
            [forma.schema :as schema]))

;; TODO: Move the next function to date-time namespace, maybe, talk
;; about this.

;; NOTE ON relative periods
;; (period->datetime "16" (+ 693 134)) => "2005-12-19"

;; (+ 693 134) => 827 defines the final period in the training data
;; set for 16-day resolution.  This period, then, defines the *first*
;; element in the probability series.  That is, the first probability
;; in the series indicates the likelihood of deforestation during the
;; training period.

(defn end-training
  [res]
  (datetime->period res "2005-12-31"))

(def sample-output-map
  [{:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 5
    :sample 10
    :prob-series (schema/timeseries-value
                  (end-training "16")
                  [0.1 0.2 0.4 0.7 0.9])
    :tres        "16"
    :sres        "500"
    :hansen      0}
   {:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 5
    :sample 9
    :prob-series (schema/timeseries-value
                  (end-training "16")
                  [0.1 0.1 0.1 0.1 0.1])
    :tres        "16"
    :sres        "500"
    :hansen      1}
   {:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 4
    :sample 10
    :prob-series (schema/timeseries-value
                  (end-training "16")
                  [0.1 0.2 0.4 0.7 0.9])
    :tres        "16"
    :sres        "500"
    :hansen      0}
   {:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 5
    :sample 11
    :prob-series (schema/timeseries-value
                  (end-training "16")
                  [0.1 0.6 0.6 0.65 0.9])
    :tres         "16"
    :sres        "500"
    :hansen       1}
   {:cntry       "MYS"
    :admin       12345
    :modh 28
    :modv 7
    :line 4
    :sample 9
    :prob-series (schema/timeseries-value
                  (end-training "16")
                  [0.1 0.2 0.4 0.7 0.9])
    :tres        "16"
    :sres        "500"
    :hansen      1}])


;; (defn latlon-series
;;   [res out-m]
;;   (modis->latlon  ))

(defn grab-modis-coords
  [m]
  (vec (map #(get m %) [:sres :modh :modv :sample :line])))

(defn grab-series
  [m]
  (:series (:prob-series m)))

(defn convert-to-latlon
  [out-m]
  (let [series (grab-series out-m)
        [res modh modv sample line] (grab-modis-coords out-m)]
    (modis->latlon res modh modv sample line)))

(def latlon-series-tap
  (<- [?lat ?lon ?series]
      (sample-output-map ?m)
      (convert-to-latlon ?m :> ?lat ?lon ?series)))

(fact
  (grab-modis-coords (first sample-output-map)) => [500 28 7 10 5])

(fact
  (grab-series (first sample-output-map)) => [0.1 0.2 0.4 0.7 0.9])

(fact
  (convert-to-latlon (first sample-output-map)) => [5])

;; (deftest adding-latlon
;;   (fact
;;     (get-latlon-series sample-output-map) => (produces [{:lat 1 :lon 1 }])))

;; (def sample-pixel-map
;;   (schema/pixel-location "500" 28 7 5 10)
;;   (modis->latlon ))

(defmapcatop grab-sig-pixels
  "return a vector of three-tuples, with the country iso code, the
  index of the period with the alert (> 0.5), and the probability of
  the alert.  If the probability of clearing never exceeds the
  threshold, then a stand-in three-tuple is returned."
  [res out-m thresh]
  (let [series (grab-series out-m)
        idx (positions #(> % thresh) series)
        offset (end-training res)]
    (if (empty? idx)
      [["nil" 0 0]]
      (vec (transpose
            [(repeat (count idx) (:cntry out-m))
             (map (partial + offset) idx)
             (map (partial nth series) idx)])))))

(def agg-tap (<- [?cntry ?pd ?tot-prob]
                 (sample-output-map ?m)
                 (grab-sig-pixels "16" ?m 0.5 :> ?cntry ?pd ?prob)
                 (c/sum ?prob :> ?tot-prob)))

(deftest agg-probs
  "test the aggregation by country and time period to simulate the
final, final step of forma, which is the country time series of
clearing activity."
  (fact
   agg-tap => (produces [["IDN" 828 0.6]
                         ["IDN" 829 0.6]
                         ["IDN" 830 2.05]
                         ["IDN" 831 2.7]
                         ["MYS" 830 0.7]
                         ["MYS" 831 0.9]
                         ["nil" 0 0]])))
