(ns forma.hadoop.jobs.trends
  (:use cascalog.api
        [incanter.stats]
        [midje sweet cascalog]
        [forma.trends.analysis :as a]
        [forma.hadoop.jobs.forma]
        [forma.matrix.utils :as mu]
        [forma.trends.filter :as f]
        [forma.matrix.utils :only (sparse-expander)])
  (:require [forma.date-time :as date]
            [forma.utils :as u]))

(defn sorted-ts
  "Accepts a map with date keys and time series values, and returns a
  vector with the values appropriately sorted.

  Example:
    (sorted-ts {:2005-12-31 3 :2006-08-21 1}) => (3 1)"
  [m]
  [(vals (into (sorted-map) m))])

(defn key->period
  [t-res k]
  (date/datetime->period t-res (name k)))

(defn period->key
  [t-res pd]
  (keyword (date/period->datetime t-res pd)))

(defn key-span
  "Returns a list of date keys, beginning and end date inclusive.  The
  first and last keys represent the periods that start just before the
  supplied boundary dates.

  Example:
    (key-span :2005-12-31 :2006-01-18 \"16\")
    => (:2005-12-19 :2006-01-01 :2006-01-17)"
  [init-key end-key t-res]
  (let [init-idx (key->period t-res init-key)
        end-idx  (inc (key->period t-res end-key))]
    (map (partial period->key t-res)
         (range init-idx end-idx))))

(defn same-len? [coll1 coll2]
  (= (count coll1) (count coll2)))

(defn all-unique?
  [coll]
  (same-len? coll (set coll)))

(defn vec->ordered-map
  "Accepts a collection and a key that indicates the first element in
  the collection, and a temporal resolution."
  [init-key t-res coll]
  (let [init (key->period t-res init-key)
        end-key  (period->key t-res (+ init (count coll)))]
    (zipmap (key-span init-key end-key t-res) coll)))

(defn update-map []
  (let [old-src [[28 8 0 0 {:2005-12-19 0 :2006-01-01 1 :2006-01-17 2}]]
        new-src [[28 8 0 0 {:2006-02-02 3 :2006-02-18 4}]]]
    (<- [?h ?v ?s ?l ?updated-map]
        (old-src ?h ?v ?s ?l ?old-map)
        (new-src ?h ?v ?s ?l ?new-map)
        (merge ?old-map ?new-map :> ?updated-map))))

(defn rand-seq [] (vec (take 281 (repeatedly rand))))

(def dyn-src [["500" 28 8 0 0 693 (rand-seq) (rand-seq) (rand-seq)]
              ["500" 28 8 0 1 693 (rand-seq) (rand-seq) (rand-seq)]
              ["500" 28 8 0 2 693 (rand-seq) (rand-seq) (rand-seq)]])

(def test-est-map {:est-start "2005-12-31"
                   :incremental-start "2012-03-22"
                   :est-end "2012-04-22"
                   :s-res "500"
                   :t-res "16"
                   :neighbors 1
                   :window-dims [600 600]
                   :vcf-limit 25
                   :long-block 30
                   :window 10
                   :ridge-const 1e-8
                   :convergence-thresh 1e-6
                   :max-iterations 500
                   :min-coast-dist 3
                   :nodata -9999.0})

(defn calc-trends
  "Calculates all trend statistics for a timeseries, also returns the
   period index of the last element of the series"
  [window long-block rain-ts ts]
  (let [short-rain (first (f/shorten-ts ts rain-ts))]
    (flatten [(a/short-stat long-block window ts)
              (a/long-stats ts short-rain)      
              (a/hansen-stat ts)])))

(defn telescoping-trends-t
  [{:keys [t-res window long-block est-end incremental-start]} start-pd rain-ts spectral-ts]
  (let [[start end] (date/relative-period t-res start-pd [incremental-start est-end])]
    (map (partial calc-trends window long-block rain-ts)
         (f/tele-ts start end spectral-ts))))

(defmapcatop trends-map
  [{:keys [incremental-start t-res] :as est-map} start-pd spectral-ts rain-ts]
  (let [start-key (keyword incremental-start)
        res (mu/transpose (telescoping-trends-t est-map start-pd rain-ts spectral-ts))]
    [(map (partial vec->ordered-map start-key t-res) res)]))

(defn analyze-trends-tester
  [{:keys [nodata long-block short-block t-res] :as est-map} dynamic-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?short ?long ?t-stat ?break]
      (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl _)
      (u/replace-from-left* nodata ?ndvi :all-types true :> ?clean-ndvi)
      (trends-map est-map ?start ?clean-ndvi ?precl :> ?short ?long ?t-stat ?break)
      (:distinct false)))

(defn no []
  (let [src (analyze-trends-tester test-est-map dyn-src)]
    (??<- [?sample ?line ?long]
          (src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?short ?long ?t-stat ?break))))
