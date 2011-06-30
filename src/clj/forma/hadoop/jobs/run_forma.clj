(ns forma.hadoop.jobs.run-forma
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set)])
  (:require [forma.matrix.walk :as w]
            [forma.date-time :as date]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.load-tseries :as tseries]
            [forma.trends.analysis :as a]
            [forma.source.modis :as modis]
            [forma.source.fire :as fire])
  (:gen-class))

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
      (io/adjust-fires est-map ?f-start ?f-series :> ?start ?fire-series)))

(defn static-tap
  "TODO: Very similar to extract-tseries. Consolidate."
  [static-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (static-src _ ?s-res _ ?tilestring ?chunkid ?chunk)
      (io/count-vals ?chunk :> ?chunk-size)
      (p/struct-index 0 ?chunk :> ?pix-idx ?val)
      (modis/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
      (modis/tile-position ?s-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defn dynamic-filter
  "Returns a new generator of ndvi and rain timeseries obtained by
  filtering out all pixels with VCF less than the supplied
  `vcf-limit`."
  [vcf-limit ndvi-src rain-src vcf-src]
  (let [vcf-pixels (static-tap vcf-src)]
    (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi-series ?precl-series]
        (ndvi-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?n-start _ ?n-series)
        (rain-src _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?r-start _ ?r-series)
        (io/adjust ?r-start ?r-series ?n-start ?n-series :> ?start ?precl-series ?ndvi-series)
        (vcf-pixels ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
        (>= ?vcf vcf-limit))))

(defn dynamic-tap
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel."
  [est-map dynamic-src]
  (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line
       ?est-start ?short-series ?long-series ?t-stat-series]
      (dynamic-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi-series ?precl-series)
      (short-trend-shell est-map ?start ?ndvi-series :> ?est-start ?short-series)
      (long-trend-shell est-map ?start ?ndvi-series ?precl-series :> ?est-start ?long-series ?t-stat-series)))

(defn forma-tap
  "Accepts an est-map and sources for ndvi, rain, and fire timeseries,
  plus a source of static vcf pixels."
  [est-map ndvi-src rain-src vcf-src fire-src]
  (let [fire-src (fire-tap est-map fire-src)
        {lim :vcf-limit} est-map
        dynamic-src (->> (dynamic-filter lim ndvi-src rain-src vcf-src)
                         (dynamic-tap est-map))]
    (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
        (dynamic-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start
                     ?short-series ?long-series ?t-stat-series)
        (fire-src ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?start !!fire-series)
        (io/forma-schema !!fire-series ?short-series ?long-series ?t-stat-series :> ?forma-series)
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

;; Here, we'll use an output tap that discards the s-res, t-res,
;; country and period fields, and writes the rest to disk.

;; TODO:

(defn integerize [& strings]
  (map #(Integer. %) strings))

(def line->nums
  (<- [?line :> ?country ?admin]
      (p/mangle #"," ?line :> ?country-s ?admin-s)
      (integerize ?country-s ?admin-s :> ?country ?admin)))

(defn country-tap
  "TODO: Very similar to extract-tseries, and almost identical to static-tap. Consolidate."
  [gadm-src convert-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?country]
      (gadm-src _ ?s-res _ ?tilestring ?chunkid ?chunk)
      (convert-src ?line)
      (line->nums ?line :> ?country ?admin)
      (io/count-vals ?chunk :> ?chunk-size)
      (p/struct-index 0 ?chunk :> ?pix-idx ?admin)
      (modis/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
      (modis/tile-position ?s-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defn forma-query
  "final query that walks the neighbors and spits out the values."
  [est-map ndvi-src rain-src vcf-src country-src fire-src]
  (let [{:keys [neighbors window-dims]} est-map
        [rows cols] window-dims
        src (-> (forma-tap est-map ndvi-src rain-src vcf-src fire-src)
                (p/sparse-windower ["?sample" "?line"] window-dims "?forma-val" nil))]
    (<- [?s-res ?t-res ?country ?datestring ?text]
        (date/period->datetime ?t-res ?period :> ?datestring)
        (src ?s-res ?t-res ?mod-h ?mod-v ?win-col ?win-row ?period ?window)
        (country-src ?s-res ?mod-h ?mod-v ?sample ?line ?country)
        (process-neighbors [neighbors] ?window :> ?win-idx ?val ?neighbor-vals)
        (modis/tile-position cols rows ?win-col ?win-row ?win-idx :> ?sample ?line)
        (io/textify ?mod-h ?mod-v ?sample ?line ?val ?neighbor-vals :> ?text))))

(defn forma-textline
  [path pathstr]
  (io/template-textline path pathstr
                        :outfields ["?text"]
                        :templatefields ["?s-res" "?t-res" "?country" "?datestring"]))

;; Hardcoded in, for the big run.
(def *ndvi-path* "s3n://redddata/ndvi/1000-32/*/*/")
(def *rain-path* "s3n://redddata/precl/1000-32/*/*/")
(def *fire-path* "s3n://redddata/fire/1000-01/*/")
(def *vcf-tap* (io/chunk-tap "s3n://redddata/"
                             ["vcf"]
                             ["1000-32"]
                             (for [[th tv] (vec (tile-set :IDN :MYS))]
                               (format "%03d%03d" th tv))))

(def *gadm-path* "s3n://redddata/gadm/1000-00/*/*/")

;; TODO: Put it there!
(def *convert-path* "s3n://modisfiles/static/conversion.csv")

(def forma-map
  {:est-start "2005-12-01"
   :est-end "2011-04-01"
   :t-res "32"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 15
   :window 5})

;; TODO: Rewrite this, so that we only need to give it a sequence of
;; countries (or tiles), and it'll generate the rest.
(defn -main
  [out-path]
  (let [ndvi-src (tseries/tseries-query *ndvi-path*)
        rain-src (tseries/tseries-query *rain-path*)
        vcf-src *vcf-tap*
        country-src (country-tap (hfs-seqfile *gadm-path*)
                                 (hfs-textline *convert-path*))
        fire-src (hfs-seqfile *fire-path*)]
    (?- (forma-textline out-path "%s/%s-%s/%s/")
        (forma-query forma-map ndvi-src rain-src vcf-src country-src fire-src))))
