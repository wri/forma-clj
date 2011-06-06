(ns forma.hadoop.jobs.run-forma
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set)])
  (:require [forma.date-time :as date]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a]
            [cascalog.ops :as c])
  (:import [forma.schema FormaValue]))

(defn adjust
  "Appropriately truncates the incoming Thrift timeseries structs, and
  outputs a new start and both truncated series."
  [& pairs]
  (let [[starts seqs] (map #(take-nth 2 %) [pairs (rest pairs)])
        distances (for [[x0 seq] (partition 2 (interleave starts seqs))]
                    (+ x0 (io/count-vals seq)))
        drop-bottoms (map #(- (apply max starts) %) starts)
        drop-tops (map #(- % (apply min distances)) distances)]
    (apply vector
           (apply max starts)
           (for [[db dt seq] (partition 3 (interleave drop-bottoms drop-tops seqs))]
             (io/to-struct (->> (io/get-vals seq)
                                (drop db)
                                (drop-last dt)))))))

(defn adjust-fires
  "Returns the section of fires data found appropriate based on the
  information in the estimation parameter map."
  [{:keys [est-start est-end t-res]} f-start f-series]
  (let [[start end] (map (partial date/datetime->period "32") [est-start est-end])]
    [start (->> (io/get-vals f-series)
                (drop (- start f-start))
                (drop-last (- (+ f-start (dec (io/count-vals f-series))) end))
                io/to-struct)]))

(defn forma-value
  [fire short long t-stat]
  (doto (FormaValue.)
    (.setFireValue fire)
    (.setShortDrop short)
    (.setLongDrop long)
    (.setTStat t-stat)))

(defn forma-schema
  "Accepts a number of timeseries of equal length and starting
  position, and converts the first entry in each timeseries to a
  `FormaValue`, for all first values and on up the sequence. Series
  must be supplied in the order specified by the arguments for
  `forma-value`."
  [& in-series]
  (def my-series in-series)
  [(->> in-series
        (map #(if % (io/get-vals %) (repeat %)))
        (apply map forma-value)
        io/to-struct)])

(defn helper []
  (->> my-series
       (map #(if % (io/get-vals %) (repeat %)))
       (apply map vector)))

(defn short-trend-shell
  "a wrapper to collect the short-term trends into a form that can be
  manipulated from within cascalog."
  [{:keys [est-start est-end t-res long-block window]} ts-start ts-series]
  (def test-start ts-start)
  (let [[start end] (date/relative-period t-res ts-start [est-start est-end])]
    [(date/datetime->period t-res est-start)
     (->> (io/get-vals ts-series)
          (a/collect-short-trend start end long-block window)
          io/to-struct)]))

(defn long-trend-shell
  "a wrapper that takes a map of options and attributes of the input
  time-series (and cofactors) to extract the long-term trends and
  t-statistics from the time-series."
  [{:keys [est-start est-end t-res long-block window]} ts-start ts-series & cofactors]
  (let [[start end] (date/relative-period t-res ts-start [est-start est-end])]
    (apply vector
           (date/datetime->period t-res est-start)
           (->> (a/collect-long-trend start end
                                      (io/get-vals ts-series)
                                      (map io/get-vals cofactors))
                (apply map (comp io/to-struct vector))))))

(def some-map
  {:est-start "2005-12-01"
   :est-end "2011-04-01"
   :t-res "32"
   :long-block 15
   :window 5})

;; TODO: we need to bring in the static data and filter on VCF.
(defn dynamic-stats
  [est-map & countries]
  (let [tiles (map tile-set countries)
        ndvi-tap (hfs-seqfile "/path/to/ndviseries")
        precl-tap (hfs-seqfile "/path/to/preclseries")
        fire-tap (hfs-seqfile "/path/to/fireseries")
        est-start (:est-start est-map)]
    (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
        (ndvi-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line !n-start _ !n-series)
        (precl-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line !p-start _ !p-series)
        (adjust !p-start !p-series !n-start !n-series :> !start !precl-series !ndvi-series)
        (short-trend-shell est-map !start !ndvi-series :> !est-start !short-series)
        (long-trend-shell est-map !start !ndvi-series !precl-series :> !est-start !long-series !t-stat-series)

        (fire-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line !f-start _ !f-series)
        (adjust-fires est-map ?f-start !f-series :> !est-start !fire-series)
        
        (forma-schema !fire-series !short-series !long-series !t-stat-series :> ?forma-series)
        (p/struct-index !est-start !forma-series :> !period !forma-val))))

;; FORMA, broken down.
(def ndvi-src [["ndvi" "1000" "32" 13 9 1199 866 370 372 (io/int-struct [3 2 1])]])
(def rain-src [["ndvi" "1000" "32" 13 9 1199 866 370 372 (io/int-struct [3 2 1])]])
(def fire-src [["ndvi" "1000" "32" 13 9 1199 867 370 372 (io/fire-series
                                                          [
                                                           (io/fire-tuple 0 0 0 0)
                                                           (io/fire-tuple 1 2 1 0)
                                                           (io/fire-tuple 1 1 1 1)
                                                           ])]])

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
       [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?forma-series]
       (dynamic-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start !!n-series !!p-series)
       (new-fire-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start !!f-series)
       (forma-schema !!f-series !!n-series !!p-series !!p-series :> ?forma-series)))

(defn run-test
  [path]
  (let [ndvi-tap (hfs-seqfile path)
        rain-tap (hfs-seqfile path)
        query (-> (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?est-start ?short-series ?long-series ?cof-series]
                      (ndvi-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start _ ?ndvi-series)
                      (rain-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start _ ?rain-series)
                      (short-trend-shell some-map ?start ?ndvi-series :> ?est-start ?short-series)
                      (long-trend-shell some-map ?start ?ndvi-series ?rain-series :> ?est-start ?long-series ?cof-series))
                  (c/first-n 1))]
    (?- (stdout) query)))

(defn tester []
  (-> (dynamic-stats some-map :IDN :MYS)
      (p/sparse-windower ["?sample" "?line"] [600 600] "?forma-val" nil)))

(defn -main [])
