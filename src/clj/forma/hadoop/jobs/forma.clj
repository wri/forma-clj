(ns forma.hadoop.jobs.forma
  (:use cascalog.api)
  (:require [cascalog.ops :as c]
            [forma.matrix.walk :as w]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a]))

;; TODO: Convert these two to Cascalog queries.

(defn short-trend-shell
  "a wrapper to collect the short-term trends into a form that can be
  manipulated from within cascalog."
  [{:keys [est-start est-end t-res long-block window]}
   spectral-series reli-series]
  (let [ts-start    (:start-idx spectral-series)
        freq        (date/res->period-count t-res)
        new-start   (date/datetime->period t-res est-start)
        [start end] (date/relative-period t-res ts-start [est-start est-end])]
    (def bad-args
      [long-block window freq start end
       (:series spectral-series)
       (:series reli-series)])
    [(->> (a/telescoping-short-trend long-block window freq start end
                                     (:series spectral-series)
                                     (:series reli-series))
          (schema/timeseries-value new-start))]))

(def spectral
  [6217 8599 7074 8437 8471 8285 8342 9035 8356 7612 8538 6439 8232 7277 8588 7651 8824 4981 7251 7332 6179 4618 6506 8188 8320 6262 8094 8129 6773 7230 6417 6791 6285 6013 5786 8020 7588 6423 5734 6522 6481 7924 8067 7328 4249 8490 8591 4472 6335 8706 8076 8376 8861 8183 8712 6426 8314 8441 6643 7673 5193 8813 7902 7275 4480 7004 5691 5630 7540 8610 8981 5181 8947 8681 9072 8931 8879 8770 8702 6578 9027 8846 8530 6927 9128 5984 6133 4775 6707 6707 3392 7081 5806 6580 9108 5748 6784 8520 8597 9130 7585 6531 6768 7249 4992 4048 7988 8088 7418 4082 8056 2715 1899 8579 8852 8896 3010 8063 7985 8377 5503 8139 8672 8319 5995 8252 8835 8593 8909 6817 8488 7206 8561 8549 4261 5659 5924 8601 7302 2610 7610 7416 8978 8704 8528 8236 5400 6372 8387 9279 9175 8652 4637 4167 5624 5707])

(def reli
  [3 3 3 0 1 0 1 1 0 1 3 3 1 1 1 3 1 3 3 3 3 3 1 1 1 3 1 0 1 3 1 1 0 0 0 1 1 0 3 3 3 1 1 1 3 1 1 3 3 1 1 1 1 1 1 1 0 0 3 1 3 1 1 3 3 1 1 3 1 0 1 3 1 3 1 1 1 1 1 1 1 0 0 3 1 3 3 3 3 3 3 1 3 3 1 1 3 1 1 1 1 1 1 1 1 1 0 1 3 3 1 3 3 1 1 1 1 0 1 3 3 1 0 1 3 0 1 1 1 3 3 3 1 0 3 1 1 1 1 3 3 3 1 1 0 1 1 3 0 1 1 1 3 3 3 1])

(let [long-block 30
      window     10
      freq       23
      start      134
      end        251]
  (a/telescoping-short-trend long-block window freq start end
                             spectral reli))

;; We're mapping across two sequences at the end, there; the
;; long-series and the t-stat-series.

(defn long-trend-shell
  "a wrapper that takes a map of options and attributes of the input
  time-series (and cofactors) to extract the long-term trends and
  t-statistics from the time-series."
  [{:keys [est-start est-end t-res long-block window]}
   ts-series reli-series rain-series]
  (let [ts-start    (:start-idx ts-series)
        freq        (date/res->period-count t-res)
        new-start   (date/datetime->period t-res est-start)
        [start end] (date/relative-period t-res ts-start [est-start est-end])]
    (apply map (comp (partial schema/timeseries-value new-start)
                     vector)
           (a/telescoping-long-trend freq start end
                                     (:series ts-series)
                                     (:series reli-series)
                                     (:series rain-series)))))

(def get-loc
  (<- [?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (map ?chunk [:location :value] :> ?location ?val)
      (schema/unpack-pixel-location ?location :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn fire-tap
  "Accepts an est-map and a query source of fire timeseries. Note that
  this won't work, pulling directly from the pail!"
  [est-map fire-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?fire-series]
      (fire-src ?chunk)
      (get-loc ?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?f-series)
      (schema/adjust-fires est-map ?f-series :> ?fire-series)))

(defn dynamic-filter
  "Returns a new generator of ndvi and rain timeseries obtained by
  filtering out all pixels with VCF less than the supplied
  `vcf-limit`."
  [vcf-limit ndvi-src reli-src rain-src vcf-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?ndvi-series ?precl-series ?reli-series]
      (ndvi-src _ ?ndvi-chunk)
      (rain-src _ ?rain-chunk)
      (vcf-src _ ?vcf-chunk)
      (reli-src _ ?reli-chunk)
      (p/blossom-chunk ?vcf-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
      (get-loc ?ndvi-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?n-series)
      (get-loc ?rain-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?r-series)
      (get-loc ?reli-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?reli)
      (schema/adjust-timeseries ?r-series ?n-series ?reli
                                :> ?precl-series ?ndvi-series ?reli-series)
      (>= ?vcf vcf-limit)))

(defn dynamic-tap
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis."
  [est-map dynamic-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line
       ?short-series ?break-series ?long-series ?t-stat-series]
      (dynamic-src
       ?s-res ?mod-h ?mod-v ?sample ?line ?ndvi-series ?precl-series ?reli-series)
      (short-trend-shell est-map ?ndvi-series ?reli-series :> ?short-series)
      (long-trend-shell est-map ?ndvi-series ?reli-series ?precl-series
                        :> ?break-series ?long-series ?t-stat-series)
      (:distinct false)))

(defn forma-tap
  "Accepts an est-map and sources for ndvi, rain, and fire timeseries,
  plus a source of static vcf pixels."
  [est-map ndvi-src reli-src rain-src vcf-src fire-src]
  (let [fire-src (fire-tap est-map fire-src)
        {lim :vcf-limit} est-map
        dynamic-src (->> (dynamic-filter lim ndvi-src reli-src rain-src vcf-src)
                         (dynamic-tap est-map))]
    (<- [?s-res ?period ?mod-h ?mod-v ?sample ?line ?forma-val]
        (fire-src ?s-res ?mod-h ?mod-v ?sample ?line !!fire-series)
        (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line
                     ?short-series ?break-series ?long-series ?t-stat-series)
        (schema/forma-seq !!fire-series ?short-series
                          ?break-series ?long-series ?t-stat-series :> ?forma-seq)
        (get ?short-series :start-idx :> ?start)
        (p/index ?forma-seq :zero-index ?start :> ?period ?forma-val)
        (:distinct false))))

;; TODO: Filter identity, instead of complement nil
(defmapcatop [process-neighbors [num-neighbors]]
  "Processes all neighbors... Returns the index within the chunk, the
value, and the aggregate of the neighbors."
  [window]
  (for [[idx [val neighbors]] (->> (w/neighbor-scan num-neighbors window)
                                   (map-indexed vector))
        :when val]
    [idx val (->> neighbors
                  (apply concat)
                  (filter (complement nil?))
                  (schema/combine-neighbors))]))

(defn forma-query
  "final query that walks the neighbors and spits out the values."
  [est-map ndvi-src reli-src rain-src vcf-src fire-src]
  (let [{:keys [neighbors window-dims]} est-map
        [rows cols] window-dims
        src (-> (forma-tap est-map ndvi-src rain-src reli-src vcf-src fire-src)
                (p/sparse-windower ["?sample" "?line"]
                                   window-dims
                                   "?forma-val"
                                   nil))]
    (<- [?s-res ?period ?mod-h ?mod-v ?sample ?line ?val ?neighbor-val]
        (src ?s-res ?period ?mod-h ?mod-v ?win-col ?win-row ?window)
        (process-neighbors [neighbors] ?window :> ?win-idx ?val ?neighbor-val)
        (r/tile-position cols rows ?win-col ?win-row ?win-idx :> ?sample ?line)
        (:distinct false))))

(defn mk-feature-vec [forma-val neighbor-val]
  (concat (schema/unpack-forma-val forma-val)
          (schema/unpack-neighbor-val neighbor-val)))

(comment
  (defn beta-extraction
    [{:keys [t-res est-start ridge-const convergence-thresh max-iterations]}
     forma-src static-src]
    (let [first-idx (date/datetime->period t-res est-start)]
      (<- [?s-res ?eco ?beta-vec]
          (forma-src ?s-res ?period ?mod-h ?mod-v ?sample ?line ?feature-vec)
          (static-src ?s-res ?period ?mod-h ?mod-v ?sample ?line ?eco ?hansen)
          (= ?period first-idx)
          (logistic-beta-wrap [ridge-const convergence-thresh max-iterations]
                              ?hansen ?feature-vec :> ?beta-vec))))

  (defn final-q [,,,]
    (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?timeseries]
        (beta-src ?s-res ?eco ?beta-vec)
        (forma-src ?s-res ?period ?mod-h ?mod-v ?sample ?line ?feature-vec)
        (static-src ?s-res ?period ?mod-h ?mod-v ?sample ?line ?eco)
        (logistic-prob ?beta-vec ?feature-vec :> ?prob)
        (mk-timeseries ?t-res ?period ?prob :> ?timeseries)))

  (let [m {:est-start "2005-12-31"
           :est-end "2012-01-17"
           :s-res "500"
           :t-res "16"
           :neighbors 1
           :window-dims [600 600]
           :vcf-limit 25
           :long-block 30
           :window 10
           :ridge-const 1e-8
           :convergence-thresh 1e-6
           :max-iterations 500}
        ndvi-src [[1 (schema/chunk-value "ndvi" "16" nil
                                         (schema/pixel-location "500" 8 6 0 0)
                                         (schema/timeseries-value 0 [1 2 3]))]]
        reli-src [[1 (schema/chunk-value "reli" "16" nil
                                         (schema/pixel-location "500" 8 6 0 0)
                                         (schema/timeseries-value 0 [1 2 3]))]]
        rain-src [[2 (schema/chunk-value "precl" "16" nil
                                         (schema/pixel-location "500" 8 6 0 0)
                                         (schema/timeseries-value 0 [1 2 3]))]]
        vcf-src  [[3 (schema/chunk-value "vcf" "00" nil
                                         (schema/chunk-location "500" 8 6 0 24000)
                                         (into [] (repeat 10 30)))]]
        fire-src [[2 (schema/chunk-value "precl" "16" nil
                                         (schema/pixel-location "500" 8 6 0 0)
                                         (schema/timeseries-value
                                          0 (repeat 3 (schema/fire-value
                                                       1 1 1 1))))]]]
    (??- (fire-tap m fire-src))
    #_(??- (forma-query m ndvi-src reli-src rain-src vcf-src fire-src))))



