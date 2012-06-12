(ns forma.hadoop.jobs.forma
  (:use cascalog.api)
  (:require [cascalog.ops :as c]
            [forma.matrix.walk :as w]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.thrift :as thrift]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a]
            [forma.ops.classify :as log]
            [forma.trends.filter :as f]))

;; We're mapping across two sequences at the end, there; the
;; long-series and the t-stat-series.

(defn fire-tap
  "Accepts an est-map and a query source of fire timeseries. Note that
  this won't work, pulling directly from the pail!"
  [est-map fire-src]
  (<- [?s-res ?h ?v ?sample ?line ?adjusted-ts]
      (fire-src ?pixel-chunk)
      (thrift/unpack ?pixel-chunk :> _ ?pixel-loc ?data _)
      (thrift/unpack ?pixel-loc :> ?s-res ?h ?v ?sample ?line)
      (thrift/unpack ?data :> ?ts)
      (schema/adjust-fires est-map ?ts :> ?adjusted-ts)))

(defn ts-unpacking
  [ts-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series]
      (ts-src _ ?ts-chunk)
      ;; unpack ts object
      (thrift/unpack ?ts-chunk :> _ ?ts-loc ?ts-data _ _)
      (thrift/unpack ?ts-data :> ?start-idx _ ?ts-array)
      (thrift/unpack* ?ts-array :> ?series)
      (thrift/unpack ?ts-loc :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn filter-query
  [vcf-src vcf-limit chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series]
      (chunk-src _ ?ts-chunk)
      (vcf-src _ ?vcf-chunk)      

      ;; mirrors p/blossom-chunk, but doesn't have "invalid predicate" error
      ;; (p/blossom-chunk ?vcf-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
      (thrift/unpack ?vcf-chunk :> _ ?vcf-loc ?vcf-data _ !date)
      (thrift/unpack* ?vcf-data :> ?vcf-vals)
      (p/index ?vcf-vals :> ?pixel-idx ?vcf)
      (thrift/unpack ?vcf-loc :> ?s-res ?mod-h ?mod-v ?id ?size)
      (r/tile-position ?s-res ?size ?id ?pixel-idx :> ?sample ?line)
      
      ;; unpack ts object
      (thrift/unpack ?ts-chunk :> _ ?ts-loc ?ts-data _ !date)
      (thrift/unpack ?ts-data :> ?start-idx _ ?ts-array)
      (thrift/unpack* ?ts-array :> ?series)
      (thrift/unpack ?ts-loc :> ?s-res ?mod-h ?mod-v ?sample ?line)

      ;; filter on vcf-limit
      (>= ?vcf vcf-limit)
      (:distinct false)))

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
        training-reli     (take start-idx reli-ts)
        training-reli-set (set training-reli)
        clean-fn (comp vector (partial f/make-clean freq good-set bad-set))]
    (if (f/reliable? good-set reli-thresh training-reli)
      (map clean-fn
           (f/tele-ts start-idx end-idx val-ts)
           (f/tele-ts start-idx end-idx reli-ts))
      [[nil]])))

(defn dynamic-clean
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis. Note that all variable names within this
  query are TIMESERIES, not individual values."
  [est-map dynamic-src]
  (let [good-set #{0 1}
        bad-set #{2 3 255}]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?clean-ndvi ?precl]
        (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl ?reli)
        (tele-clean est-map good-set bad-set ?start ?ndvi ?reli :> ?clean-ndvi)
        (:distinct false))))

(defmapop series-end
  [series start]
  (let [length (count series)]
    (dec (+ start length))))

(defn analyze-trends
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis. Note that all variable names within this
  query are TIMESERIES, not individual values."
  [est-map clean-src]
  (let [long-block (:long-block est-map)
        short-block (:window est-map)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?end ?short ?long ?t-stat ?break]
        (clean-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl)
        (f/shorten-ts ?ndvi ?precl :> ?short-precl)
        (a/short-stat long-block short-block ?ndvi :> ?short)
        (a/long-stats ?ndvi ?short-precl :> ?long ?t-stat)
        (a/hansen-stat ?ndvi :> ?break)
        (series-end ?ndvi ?start :> ?end)
        (:distinct false))))

(defn count-series
  [series]
  (count (first series)))

(defbufferop consolidator
  [tuples]
  [[[(vec (map first tuples))]
    [(vec (map #(nth % 1) (sort-by first tuples)))]
    [(vec (map #(nth % 2) (sort-by first tuples)))]
    [(vec (map #(nth % 3) (sort-by first tuples)))]
    [(vec (map #(nth % 4) (sort-by first tuples)))]]])

(defn get-max-period
  [series]
  (reduce max (first series)))

(defn consolidate-trends
  [trends-src]
  (<- [?s-res ?mh ?mv ?s ?l ?start ?end-seq ?short-ts ?long-ts ?t-stat-ts ?break-ts]
      (trends-src ?s-res ?mh ?mv ?s ?l ?start ?end ?short ?long ?t-stat ?break)
      (consolidator ?end ?short ?long ?t-stat ?break :> ?end-seq ?short-ts ?long-ts ?t-stat-ts ?break-ts)))

(defn trends-cleanup
  [trends-src]
  (let [consolidated-tap (consolidate-trends trends-src)]
    (<- [?s-res ?mh ?mv ?s ?l ?start ?end ?short-ts ?long-ts ?t-stat-ts ?break-ts]
        (consolidated-tap ?s-res ?mh ?mv ?s ?l ?start ?end-seq ?short-ts ?long-ts ?t-stat-ts ?break-ts)
      (get-max-period ?end-seq :> ?end))))

(defn unnest-series
  "Remove extraneous vectors from around a series"
  [series]
  (vec (flatten series)))

(defn unnest-all
  "Unnest series coming out of trends-cleanup of form
   [[\"500\" 28 8 0 0 693 694 [[0.1 0.1]] [[0.2 0.2]] [[0.3 0.3]] [[0.4 0.4]]]]"
  [& series]
  (map unnest-series series))

(defn forma-tap
  "Accepts an est-map and sources for 

  Note that all values internally discuss timeseries.

  Also note that !!fire is an ungrounding variable, and triggers a left join
  with the trend result variables."
  [dynamic-src fire-src]
  (<- [?s-res ?period ?mh ?mv ?s ?l ?forma-val]
      (fire-src ?s-res ?mh ?mv ?s ?l !!fire)
      (dynamic-src ?s-res ?mh ?mv ?s ?l ?start ?end ?short ?long ?t-stat ?break)
      (unnest-all ?short ?long ?t-stat ?break :> ?short-v ?long-v ?t-stat-v ?break-v)
      (schema/forma-seq-non-thrift !!fire ?short-v ?long-v ?t-stat-v ?break-v :> ?forma-seq)
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

(defn beta-data-prep
  [{:keys [t-res est-start min-coast-dist]} dynamic-src static-src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?eco ?hansen]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco ?hansen ?border)
        (= ?pd first-idx)
        (>= ?border min-coast-dist)
        (:distinct false))))

(defn beta-gen
  "query to return the beta vector associated with each ecoregion"
  [{:keys [t-res est-start ridge-const convergence-thresh max-iterations min-coast-dist]} src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?eco ?beta]
        (src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?eco ?hansen)
        (log/logistic-beta-wrap [ridge-const convergence-thresh max-iterations]
                                ?hansen ?val ?neighbor-val :> ?beta)
        (:distinct false))))

(defn beta-generator
  "query to return the beta vector associated with each ecoregion"
  [{:keys [t-res est-start ridge-const convergence-thresh max-iterations min-coast-dist]}
   dynamic-src static-src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?eco ?beta]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco ?hansen ?border)
        (= ?pd first-idx)
        (>= ?border min-coast-dist)
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
  [beta-src dynamic-src static-src period]
  (let [betas (log/beta-dict beta-src)]
    (<- [?s-res ?mod-h ?mod-v ?s ?l ?prob-series]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco _ _)
        (= period ?pd)
        (apply-betas [betas] ?eco ?val ?neighbor-val :> ?prob)
        (log/mk-timeseries ?pd ?prob :> ?prob-series)
        (:distinct false))))

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
