(ns forma.hadoop.jobs.trends
  (:use cascalog.api
        [incanter.stats]
        [midje sweet cascalog]
        [forma.trends.analysis :as a]
        [forma.trends.filter :as f])
  (:require [forma.date-time :as date]
            [forma.utils :as u]))

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

(def test-est-map {:data-start "2000-02-17"
                   :est-start "2005-12-31"
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


