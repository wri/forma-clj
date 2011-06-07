(ns forma.hadoop.jobs.run-forma
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
  (:import [forma.schema FormaValue FormaNeighborValue]))

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

(defn forma-schema
  "Accepts a number of timeseries of equal length and starting
  position, and converts the first entry in each timeseries to a
  `FormaValue`, for all first values and on up the sequence. Series
  must be supplied in the order specified by the arguments for
  `forma.hadoop.io/forma-value`."
  [& in-series]
  (def my-series in-series)
  [(->> in-series
        (map #(if % (io/get-vals %) (repeat %)))
        (apply map io/forma-value)
        io/to-struct)])

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
      (modis/tilestring->hv ?tilestring :> ?tile-h ?tile-v)
      (modis/tile-position ?s-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

;; TODO: Filter by VCF. Do it here... that'll take care of everything.
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

(defn forma-tap
  "Accepts an est-map and sources for ndvi, rain, and fire timeseries,
  plus a source of static vcf pixels."
  [est-map ndvi-src rain-src vcf-src fire-src]
  (let [dynamic-src (dynamic-tap est-map ndvi-src rain-src vcf-src)
        fire-src (fire-tap est-map fire-src)]
    (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
        (dynamic-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start
                     ?short-series ?long-series ?t-stat-series)
        (fire-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start !!fire-series)
        (forma-schema !!fire-series ?short-series ?long-series ?t-stat-series :> ?forma-series)
        (p/struct-index ?start ?forma-series :> ?period ?forma-val))))

;; TODO: Implement a function that merges a FormaValue into a
;; FormaNeighborhoodValue, and reduce with that.
(defn combine-neighbors
  "Combines a sequence of neighbors into the final average value that
  Dan needs."
  [forma-val-seq]
  (let [[_ & one-vals] (io/unpack-forma-val (first forma-val-seq))
        [fire sums mins] (reduce (fn [[f-tuple sums mins] val]
                                   (let [[fire & fields] (io/unpack-forma-val val)]
                                     [(io/add-fires f-tuple fire)
                                      (map + sums fields)
                                      (map min mins fields)]))
                                 [(io/fire-tuple 0 0 0 0) [0 0 0] one-vals]
                                 forma-val-seq)
        num-neighbors (count forma-val-seq)
        [avg-short avg-long avg-stat] (map #(/ % num-neighbors) sums)
        [min-short min-long min-stat] mins]
    (FormaNeighborValue. fire
                         num-neighbors
                         avg-short min-short
                         avg-long min-long
                         avg-stat min-stat)))

;; Processes all neighbors... Returns the index within the chunk, the
;; value, and the aggregate of the neighbors.

(defmapcatop [process-neighbors [num-neighbors]]
  [window]
  (->> (for [[val neighbors] (w/neighbor-scan num-neighbors window) :when val]
         [val (combine-neighbors (->> neighbors
                                      (apply concat)
                                      (filter (complement nil?))))])
       (map-indexed cons)))

;; TODO: This could be combined with the distance stuff we did for the
;; sinusoidal projection. Look at the modis namespace.
(defn pixel-position
  [num-cols num-rows win-col win-row idx]
  (println "HELP!")
  (let [[ row col] (idx->rowcol num-rows num-cols idx)]
       [(+ col (* num-cols win-col))
        (+ row (* num-rows win-row))]))

;; TODO: Merge in the country dataset for bucketing.
(defn extract-fields
  "Returns a vector containing the value of the `temp330`, `conf50`,
  `bothPreds` and `count` fields of the supplied `FireTuple` thrift
  object."  [firetuple]
  [(.temp330 firetuple)
   (.conf50 firetuple)
   (.bothPreds firetuple)
   (.count firetuple)])

(defn textify
  "Converts the supplied coordinates, `FormaValue` and
  `FormaNeighborValue` into a line of text suitable for use in STATA."
  [mod-h mod-v sample line ^FormaValue val ^FormaNeighborValue neighbor-vals]
  (let [[fire-val s-drop l-drop t-drop] (io/unpack-forma-val val)
        [k330 c50 ck fire] (io/extract-fields fire-val)
        [k330-n c50-n ck-n fire-n] (io/extract-fields
                                    (.getFireValue neighbor-vals))]
    (s/join " "
            [mod-h mod-v sample line
             k330 c50 ck fire
             s-drop l-drop t-drop
             k330-n c50-n ck-n fire-n
             (.getAvgShortDrop neighbor-vals)
             (.getMinShortDrop neighbor-vals)
             (.getAvgLongDrop neighbor-vals)
             (.getMinLongDrop neighbor-vals)
             (.getAvgTStat neighbor-vals)
             (.getMinTStat neighbor-vals)])))

(defn forma-query
  "final query that walks the neighbors and spits out the values."
  [est-map ndvi-src rain-src vcf-src country-src fire-src]
  (let [{:keys [neighbors window-dims]} est-map
        [rows cols] window-dims
        country-pixels (static-tap country-src)
        src (-> (forma-tap est-map ndvi-src rain-src vcf-src fire-src)
                (p/sparse-windower ["?sample" "?line"] window-dims "?forma-val" nil))]
    (<- [?s-res ?t-res ?country ?datestring ?text-line]
        (date/period->datetime ?t-res ?period :> ?datestring)
        (src ?s-res ?t-res ?mod-h ?mod-v ?win-col ?win-row ?period ?window)
        (country-pixels ?s-res ?mod-h ?mod-v ?sample ?line ?country)
        (process-neighbors [neighbors] ?window :> ?win-idx ?val ?neighbor-vals)
        (pixel-position cols rows ?win-col ?win-row ?win-idx :> ?sample ?line)
        (textify ?mod-h ?mod-v ?sample ?line ?val ?neighbor-vals :> ?text-line))))

;; Here, we'll use an output tap that discards the s-res, t-res,
;; country and period fields, and writes the rest to disk.
(defn -main [])
