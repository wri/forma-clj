(ns forma.hadoop.jobs.forma
  (:use cascalog.api)
  (:require [cascalog.ops :as c]
            [forma.matrix.walk :as w]
            [forma.date-time :as date]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a]
            [forma.source.modis :as modis]))

(defn short-trend-shell
  "a wrapper to collect the short-term trends into a form that can be
  manipulated from within cascalog."
  [{:keys [est-start est-end t-res long-block window]} ts-series]
  (let [ts-start (io/get-start-idx ts-series)
        [start end] (date/relative-period t-res ts-start [est-start est-end])]
    [(->> (io/get-vals ts-series)
          (a/collect-short-trend start end long-block window)
          io/to-struct
          io/mk-array-value
          (io/timeseries-value start))]))

(defn long-trend-shell
  "a wrapper that takes a map of options and attributes of the input
  time-series (and cofactors) to extract the long-term trends and
  t-statistics from the time-series."
  [{:keys [est-start est-end t-res long-block window]} ts-series & cofactors]
  (let [ts-start (io/get-start-idx ts-series)
        [start end] (date/relative-period t-res ts-start [est-start est-end])]
    (->> (a/collect-long-trend start end
                               (io/get-vals ts-series)
                               (map io/get-vals cofactors))
         (apply map (comp (partial io/timeseries-value start)
                          io/mk-array-value
                          io/to-struct
                          vector)))))

(defn fire-tap
  "Accepts an est-map and a query source of fire timeseries. Note that
  this won't work, pulling directly from the pail!"
  [est-map fire-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?fire-series]
      (fire-src ?chunk)
      ((c/juxt #'io/extract-location
               #'io/extract-chunk-value) ?chunk :> ?location ?f-series)
      (io/get-pos ?location :> ?s-res ?mod-h ?mod-v ?sample ?line)
      (io/adjust-fires est-map ?f-series :> ?fire-series)))

(defn dynamic-filter
  "Returns a new generator of ndvi and rain timeseries obtained by
  filtering out all pixels with VCF less than the supplied
  `vcf-limit`."
  [vcf-limit ndvi-src rain-src vcf-src]
  (let [extracter (c/juxt #'io/extract-location
                          #'io/extract-chunk-value)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?ndvi-series ?precl-series]
        (ndvi-src _ ?ndvi-chunk)
        (rain-src _ ?rain-chunk)
        (vcf-src _ ?vcf-chunk)
        (p/blossom-chunk ?vcf-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
        (extracter ?ndvi-chunk :> ?location ?n-series)
        (extracter ?rain-chunk :> ?location ?r-series)
        (io/get-pos ?location :> ?s-res ?mod-h ?mod-v ?sample ?line)
        (io/adjust-timeseries ?r-series ?n-series :> ?precl-series ?ndvi-series)
        (>= ?vcf vcf-limit))))

(defn dynamic-tap
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis."
  [est-map dynamic-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?short-series ?long-series ?t-stat-series]
      (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line ?ndvi-series ?precl-series)
      (short-trend-shell est-map ?ndvi-series :> ?short-series)
      (long-trend-shell est-map
                        ?ndvi-series
                        ?precl-series :> ?long-series ?t-stat-series)))

(defn forma-tap
  "Accepts an est-map and sources for ndvi, rain, and fire timeseries,
  plus a source of static vcf pixels."
  [est-map ndvi-src rain-src vcf-src fire-src]
  (let [fire-src (fire-tap est-map fire-src)
        {lim :vcf-limit} est-map
        dynamic-src (->> (dynamic-filter lim ndvi-src rain-src vcf-src)
                         (dynamic-tap est-map))]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
        (fire-src ?s-res ?mod-h ?mod-v ?sample ?line !!fire-series)
        (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line ?short-series ?long-series ?t-stat-series)
        (io/forma-schema !!fire-series
                         ?short-series
                         ?long-series
                         ?t-stat-series :> ?forma-series)
        (io/get-start-idx ?short-series :> ?start)
        (p/struct-index ?start ?forma-series :> ?period ?forma-val))))

;; Processes all neighbors... Returns the index within the chunk, the
;; value, and the aggregate of the neighbors.
(defmapcatop [process-neighbors [num-neighbors]]
  [window]
  (->> (for [[val neighbors] (w/neighbor-scan num-neighbors window)
             :when val]
         [val (->> neighbors
                   (apply concat)
                   (filter (complement nil?))
                   (io/combine-neighbors))])
       (map-indexed cons)))

(defn forma-query
  "final query that walks the neighbors and spits out the values."
  [est-map ndvi-src rain-src vcf-src country-src fire-src]
  (let [{:keys [t-res neighbors window-dims]} est-map
        [rows cols] window-dims
        src (-> (forma-tap est-map ndvi-src rain-src vcf-src fire-src)
                (p/sparse-windower ["?sample" "?line"] window-dims "?forma-val" nil))]
    (<- [?s-res ?country ?datestring ?mod-h ?mod-v ?sample ?line ?text]
        (date/period->datetime t-res ?period :> ?datestring)
        (src ?s-res ?mod-h ?mod-v ?win-col ?win-row ?period ?window)
        (country-src ?s-res ?mod-h ?mod-v ?sample ?line ?country)
        (process-neighbors [neighbors] ?window :> ?win-idx ?val ?neighbor-vals)
        (modis/tile-position cols rows ?win-col ?win-row ?win-idx :> ?sample ?line)
        (io/textify ?val ?neighbor-vals :> ?text)
        (:distinct false))))
