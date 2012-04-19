(ns forma.hadoop.jobs.forma
  (:use cascalog.api)
  (:require [cascalog.ops :as c]
            [forma.matrix.walk :as w]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a]
            [forma.ops.classify :as log]))
            [forma.trends.filter :as f]))

;; We're mapping across two sequences at the end, there; the
;; long-series and the t-stat-series.

(def get-loc
  (<- [?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (map ?chunk [:location :value] :> ?loc ?val)
      (schema/unpack-pixel-location ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn fire-tap
  "Accepts an est-map and a query source of fire timeseries. Note that
  this won't work, pulling directly from the pail!"
  [est-map fire-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?fire-series]
      (fire-src ?chunk)
      (get-loc ?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?f-series)
      (schema/adjust-fires est-map ?f-series :> ?fire-series)))

(defn filter-query [vcf-src vcf-limit chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?ts]
      (chunk-src _ ?ts-chunk)
      (vcf-src _ ?vcf-chunk)
      (get-loc ?ts-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?series)
      (:distinct false)
      (map ?series [:start-idx :series] :> ?start ?ts)
      (p/blossom-chunk ?vcf-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
      (>= ?vcf vcf-limit)))

(defn dynamic-filter
  "Returns a new generator of ndvi and rain timeseries obtained by
  filtering out all pixels with VCF less than the supplied
  `vcf-limit`."
  [ndvi-src reli-src rain-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?ndvi-ts ?precl-ts ?reli-ts]
      (ndvi-src ?s-res ?mod-h ?mod-v ?sample ?line ?n-start ?ndvi)
      (reli-src ?s-res ?mod-h ?mod-v ?sample ?line ?r-start ?reli)
      (rain-src ?s-res ?mod-h ?mod-v ?sample ?line ?p-start ?precl)
      (schema/adjust ?p-start ?precl ?n-start ?ndvi ?r-start ?reli
                     :> ?start-idx ?precl-ts ?ndvi-ts ?reli-ts)
      (:distinct false)))

(defmapcatop tele-clean
  "Return clean timeseries with telescoping window, nil if no (or not enough) good training data"
  [{:keys [est-start est-end t-res]}
   good-set bad-set start-period val-ts reli-ts]
  (let [reli-thresh 0.1
        freq (date/res->period-count t-res)
        [start-idx end-idx] (date/relative-period t-res start-period
                                                  [est-start est-end])
        training-reli (take start-idx reli-ts)
        training-reli-set (set training-reli)
        clean-fn (comp vector (partial f/make-clean freq good-set bad-set))]
    (cond (f/reliable?
           good-set reli-thresh training-reli) (map clean-fn
                                                (f/tele-ts start-idx end-idx val-ts)
                                                (f/tele-ts start-idx end-idx reli-ts))
          :else [[nil]])))

(defn dynamic-clean
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis. Note that all variable names within this
  query are TIMESERIES, not individual values."
  [est-map dynamic-src]
  (let [good-set #{0 1}
        bad-set #{2 3 255}]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?clean-ndvi]
        (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi _ ?reli)
        (tele-clean est-map good-set bad-set ?start ?ndvi ?reli :> ?clean-ndvi)
        (:distinct false))))

(defn analyze-trends
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis. Note that all variable names within this
  query are TIMESERIES, not individual values."
  [est-map clean-src rain-src]
  (let [long-block (est-map :long-block)
        short-block (est-map :window)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?short ?long ?t-stat ?break]
        (rain-src ?s-res ?mod-h ?mod-v ?sample ?line ?start _ ?precl _)
        (f/shorten-ts ?ndvi ?precl :> ?short-precl)
        (clean-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi)
        (a/short-stat long-block short-block ?ndvi :> ?short)
        (a/long-stats ?ndvi ?short-precl :> ?long ?t-stat)
        (a/hansen-stat ?ndvi :> ?break)
        (:distinct false))))

(defn forma-tap
  "Accepts an est-map and sources for ndvi, rain, and fire timeseries,
  plus a source of static vcf pixels.

  Note that all values internally discuss timeseries."
  [dynamic-src fire-src]
  (<- [?s-res ?period ?mh ?mv ?s ?l ?forma-val]
      (fire-src ?s-res ?mh ?mv ?s ?l !!fire)
      (dynamic-src ?s-res ?mh ?mv ?s ?l ?start ?short ?break ?long ?t-stat)
      (schema/forma-seq !!fire ?short ?break ?long ?t-stat :> ?forma-seq)
      (p/index ?forma-seq :zero-index ?start :> ?period ?forma-val)
      (:distinct false)))

(defmapcatop [process-neighbors [num-neighbors]]
  "Processes all neighbors... Returns the index within the chunk, the
value, and the aggregate of the neighbors."

  [window]
  (for [[idx [val neighbors]] (->> (w/neighbor-scan num-neighbors window)
                                   (map-indexed vector))
        :when val]
    [idx val (->> neighbors
                  (apply concat)
                  (filter identity)
                  (schema/combine-neighbors))]))

(defn forma-query
  "final query that walks the neighbors and spits out the values."
  [est-map forma-val-src]
  (let [{:keys [neighbors window-dims]} est-map
        [rows cols] window-dims
        src (p/sparse-windower forma-val-src
                               ["?sample" "?line"]
                               window-dims
                               "?forma-val"
                               nil)]
    (<- [?s-res ?period ?mod-h ?mod-v ?sample ?line ?val ?neighbor-val]
        (src ?s-res ?period ?mod-h ?mod-v ?win-col ?win-row ?window)
        (process-neighbors [neighbors] ?window :> ?win-idx ?val ?neighbor-val)
        (r/tile-position cols rows ?win-col ?win-row ?win-idx :> ?sample ?line)
        (:distinct false))))

(defn beta-generator
  "query to return the beta vector associated with each ecoregion"
  [{:keys [t-res est-start ridge-const convergence-thresh max-iterations]}
   dynamic-src static-src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?eco ?beta]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco ?hansen)
        (= ?pd first-idx)
        (log/logistic-beta-wrap [ridge-const convergence-thresh max-iterations]
                                ?hansen ?val ?neighbor-val :> ?beta)
        (:distinct false))))

(defmapop [apply-betas [betas]]
  [eco val neighbor-val]
  (let [beta ((log/mk-key eco) betas)]
    (log/logistic-prob-wrap beta val neighbor-val)))

(defn forma-estimate
  "query to end all queries: estimate the probabilities for each
  period after the training period."
  [beta-src dynamic-src static-src trap-tap period]
  (let [betas (log/beta-dict beta-src)]
    (<- [?s-res ?mod-h ?mod-v ?s ?l ?prob-series]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco _)
        (apply-betas [betas] ?eco ?val ?neighbor-val :> ?prob)
        (log/mk-timeseries ?pd ?prob :> ?prob-series)
        (:distinct false)
        ;;(= ?pd period)
        (:distinct false)
        (:trap trap-tap))))

(comment
  (let [m {:est-start "2005-12-31"
           :est-end "2010-01-17"
           :s-res "500"
           :t-res "16"
           :neighbors 1
           :window-dims [600 600]
           :vcf-limit 25
           :long-block 30
           :window 10
           :ridge-const 1e-8
           :convergence-thresh 1e-10
           :max-iterations 500}
        ndvi-src [[1 (schema/chunk-value
                      "ndvi" "32" nil
                      (schema/pixel-location "500" 8 6 0 0)
                      (schema/timeseries-value 693 (concat ndvi ndvi)))]]
        reli-src [[1 (schema/chunk-value
                      "reli" "32" nil
                      (schema/pixel-location "500" 8 6 0 0)
                      (schema/timeseries-value 693 (concat reli reli)))]]
        rain-src [[2 (schema/chunk-value
                      "precl" "32" nil
                      (schema/pixel-location "500" 8 6 0 0)
                      (schema/timeseries-value 693 (concat rain-raw rain-raw)))]]
        vcf-src  [[3 (schema/chunk-value
                      "vcf" "00" nil
                      (schema/chunk-location "500" 8 6 0 24000)
                      (into [] (repeat 156 30)))]]
        fire-src [[(schema/chunk-value
                    "precl" "32" nil
                    (schema/pixel-location "500" 8 6 0 0)
                    (schema/timeseries-value
                     693 (repeat 312 (schema/fire-value
                                      1 1 1 1))))]]]
    (??- (forma-tap m ndvi-src reli-src rain-src vcf-src fire-src))))
