(ns forma.hadoop.jobs.test-forma
  (:use cascalog.api
        [forma.matrix.utils :only (idx->rowcol)]
        [forma.source.tilesets :only (tile-set)])
  (:require [cascalog.ops :as c]
            [clojure.string :as s]
            [forma.matrix.walk :as w]
            [forma.date-time :as date]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.source.modis :as modis]
            [forma.trends.analysis :as a])
  (:import [forma.schema FormaValue FormaNeighborValue])
  (:gen-class))

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

;; 361	495	360	503	361	133	133
(let [[c a b] (adjust 361 (io/int-struct (range (inc (- 495 361))))
                      360 (io/double-struct (range (inc (- 503 360)))))]
  [(io/count-vals a) (io/count-vals b)])

;; 361	495	360	502	361	135	135
(let [[c a b] (adjust 361 (io/int-struct (range (inc (- 495 361))))
                      360 (io/double-struct (range (inc (- 502 360)))))]
  [(io/count-vals a) (io/count-vals b)])

(defn adjust-fires
  "Returns the section of fires data found appropriate based on the
  information in the estimation parameter map."
  [{:keys [est-start est-end t-res]} f-start f-series]
  (let [[start end] (map (partial date/datetime->period "32") [est-start est-end])]
    [start (->> (io/get-vals f-series)
                (drop (- start f-start))
                (drop-last (- (+ f-start (dec (io/count-vals f-series))) end))
                io/to-struct)]))

(defn forma-schema
  "Accepts a number of timeseries of equal length and starting
  position, and converts the first entry in each timeseries to a
  `FormaValue`, for all first values and on up the sequence. Series
  must be supplied in the order specified by the arguments for
  `forma.hadoop.io/forma-value`."
  [& in-series]
  [(->> in-series
        (map #(if % (io/get-vals %) (repeat %)))
        (apply map io/forma-value)
        io/to-struct)])

(defn short-trend-shell
  "a wrapper to collect the short-term trends into a form that can be
  manipulated from within cascalog."
  [{:keys [est-start est-end t-res long-block window]} ts-start ts-series]
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

(defn fire-tap
  "Accepts an est-map and a source of fire timeseries."
  [est-map fire-src]
  (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?fire-series]
      (fire-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?f-start _ ?f-series)
      (adjust-fires est-map ?f-start ?f-series :> ?start ?fire-series)))

(defn static-tap
  "TODO: Very similar to extract-tseries. Consolidate."
  [static-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (static-src _ ?s-res _ ?tilestring ?chunkid ?chunk)
      (io/count-vals ?chunk :> ?chunk-size)
      (p/struct-index 0 ?chunk :> ?pix-idx ?val)
      (modis/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
      (modis/tile-position ?s-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defn dynamic-tap
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel."
  [{:keys [vcf-limit] :as est-map} ndvi-src rain-src vcf-src]
  (let [vcf-pixels (static-tap vcf-src)]
    (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?est-start
         ?short-series ?long-series ?t-stat-series]
        (ndvi-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?n-start _ ?n-series)
        (rain-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?r-start _ ?r-series)
        (vcf-pixels ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
        (> ?vcf vcf-limit)
        (adjust ?r-start ?r-series ?n-start ?n-series :> ?start ?precl-series ?ndvi-series)
        (short-trend-shell est-map ?start ?ndvi-series :> ?est-start ?short-series)
        (long-trend-shell est-map ?start ?ndvi-series ?precl-series :> ?est-start ?long-series ?t-stat-series))))

(defn or-134-135 [x]
  (or (= x 134)
      (= x 133)))

(defn dynamic-test
  [est-map ndvi-src rain-src]
  (<- [?mod-h ?mod-v ?sample ?line ?n-start ?n-end ?r-start ?r-end ?start ?precl-count ?ndvi-count
       ;; ?mod-h ?mod-v ?sample ?line ?ndvi-count
       ;; ?n-start ?n-end ?n-series
       ;; ?r-start ?r-end ?r-series
       ;; ?start ?ndvi-series ?precl-series
       ]
      (ndvi-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?n-start ?n-end ?n-series)
      (rain-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?r-start ?r-end ?r-series)
      (adjust ?r-start ?r-series ?n-start ?n-series :> ?start ?precl-series ?ndvi-series)
      (io/count-vals ?precl-series :> ?precl-count)
      (io/count-vals ?ndvi-series :> ?ndvi-count)
      ;; (or-134-135 ?ndvi-count)
      ))

(def forma-map
  {:est-start "2005-12-01"
   :est-end "2011-04-01"
   :t-res "32"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 15
   :window 5})

(defn -main
  [type ndvi-series-path rain-series-path out-path]
  (case type
        "test" (?- (hfs-seqfile out-path)
                   (dynamic-tap forma-map
                                (hfs-seqfile ndvi-series-path)
                                (hfs-seqfile rain-series-path)
                                (hfs-seqfile "s3n://redddata/vcf/*/*/*/")))
        "real" (?- (hfs-textline out-path)
                   (dynamic-test forma-map
                                 (hfs-seqfile ndvi-series-path)
                                 (hfs-seqfile rain-series-path)))
        "real2" (?- (hfs-textline out-path)
                    (dynamic-test {:est-start "2005-12-01"
                                   :est-end "2010-12-01"
                                   :t-res "32"
                                   :neighbors 1
                                   :window-dims [600 600]
                                   :vcf-limit 25
                                   :long-block 15
                                   :window 5}
                                  (hfs-seqfile ndvi-series-path)
                                  (hfs-seqfile rain-series-path)))
        "real3" (?- (hfs-textline out-path)
                    (dynamic-test {:est-start "2005-12-01"
                                   :est-end "2010-12-01"
                                   :t-res "32"
                                   :neighbors 1
                                   :window-dims [600 600]
                                   :vcf-limit 25
                                   :long-block 15
                                   :window 5}
                                  (hfs-seqfile ndvi-series-path)
                                  (hfs-seqfile rain-series-path)))))
