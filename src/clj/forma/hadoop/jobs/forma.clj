(ns forma.hadoop.jobs.forma
  (:use cascalog.api
        [forma.hadoop.pail :only (split-chunk-tap)])
  (:require [cascalog.ops :as c]
            [forma.matrix.walk :as w]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.thrift :as thrift]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a]
            [forma.ops.classify :as log]
            [forma.trends.filter :as f]
            [forma.utils :as u]
            [forma.source.humidtropics :as humid]
            [forma.matrix.utils :as mu]))

(defn consolidate-static
  "Due to an issue with Pail, we consolidate separate sequence files
   of static data with one big join.

   We'll screen out border pixels later - doing it here will remove non-water
   but nearly water pixels before they can be included as neighbors
"
  [vcf-limit vcf-src gadm-src hansen-src ecoid-src border-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?vcf ?gadm ?ecoid ?hansen ?coast-dist]
      (vcf-src    ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
      (hansen-src ?s-res ?mod-h ?mod-v ?sample ?line ?hansen)
      (ecoid-src  ?s-res ?mod-h ?mod-v ?sample ?line ?ecoid)
      (gadm-src   ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
      (border-src ?s-res ?mod-h ?mod-v ?sample ?line ?coast-dist)
      (>= ?vcf vcf-limit)
      (humid/in-humid-tropics? ?ecoid)))

(defn within-tileset?
  [tile-set h v]
  (let [tile [h v]]
    (contains? tile-set tile)))

(defn screen-by-tileset
  [src tile-set]
  (<- [?pail-path ?pixel-chunk]
      (src ?pail-path ?pixel-chunk)
      (thrift/unpack ?pixel-chunk :> _ ?pixel-loc _ _ _)
      (thrift/unpack ?pixel-loc :> _ ?h ?v _ _)
      (within-tileset? tile-set ?h ?v)))

(defn fire-tap
  "Accepts an est-map and a query source of fire timeseries. Note that
  this won't work, pulling directly from the pail!"
  [est-map fire-src]
  (<- [?s-res ?h ?v ?sample ?line ?adjusted-ts]
      (fire-src ?fire-pixel)
      (thrift/unpack ?fire-pixel :> _ ?pixel-loc ?ts _ _)
      (thrift/unpack ?pixel-loc :> ?s-res ?h ?v ?sample ?line)
      (schema/adjust-fires est-map ?ts :> ?adjusted-ts)))

(defn filter-query
  "Use a join with `static-src` - already filtered by VCF and
  ecoregion - to keep only pixels from `chunk-src` where VCF >= 25 and
  which fall inside the humid tropics.

   Arguments:
     static-src: source of tuples of static data
     vcf-limit: minimum VCF value required to keep a tuple
     chunk-src: source of timeseries chunk tuples"
  [static-src vcf-limit chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series]
      (chunk-src _ ?ts-chunk)
      (static-src ?s-res ?mod-h ?mod-v ?sample ?line ?vcf _ _ _ _)
      
      ;; unpack ts object
      (thrift/unpack ?ts-chunk :> _ ?ts-loc ?ts-data _ _)
      (thrift/unpack ?ts-data :> ?start-idx _ ?ts-array)
      (thrift/unpack* ?ts-array :> ?series)
      (thrift/unpack ?ts-loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
      
      ;; filter on vcf-limit - ensures join & filter actually happens
      (>= ?vcf vcf-limit)))

(defn training-3000s?
  "Returns true if all values in the training period are -3000s"
  [t-res start-idx est-start-dt ts]
  (let [est-start-idx (date/datetime->period t-res est-start-dt)
        training-vals (take (inc (- est-start-idx start-idx)) ts)]
    (every? (partial = -3000) training-vals)))

(defn dynamic-filter
  "Filters out all NDVI pixels where timeseries is all -3000s. Trims ndvi, reli
   and rain timeseries so that they are the same length"
  [est-map ndvi-src reli-src rain-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?ndvi-ts ?precl-ts ?reli-ts]
      (ndvi-src ?s-res ?mod-h ?mod-v ?sample ?line ?n-start ?ndvi)
      (reli-src ?s-res ?mod-h ?mod-v ?sample ?line ?r-start ?reli)
      (rain-src ?s-res ?mod-h ?mod-v ?sample ?line ?p-start ?precl)
      (training-3000s? (:t-res est-map) ?n-start (:est-start est-map) ?ndvi :> false)
      (schema/adjust ?p-start ?precl ?n-start ?ndvi ?r-start ?reli
                     :> ?start-idx ?precl-ts ?ndvi-ts ?reli-ts)))

(defmapcatop tele-clean
  "Return clean timeseries with telescoping window, nil if no (or not
  enough) good training data"
  [{:keys [est-start est-end t-res]} good-set bad-set start-period val-ts reli-ts]
  (let [reli-thresh 0.1
        freq (date/res->period-count t-res)
        [start-idx end-idx] (date/relative-period t-res start-period
                                                  [est-start est-end])
        training-reli     (take start-idx reli-ts)
        training-reli-set (set training-reli)
        clean-fn (comp vector (partial f/make-clean freq good-set bad-set))]
    (if (f/reliable? good-set reli-thresh training-reli)
      (vec (map vector (f/tele-ts start-idx end-idx val-ts)))
      [[nil]])))

(defmapcatop telescope-ts
  "Telescope a timeseries. Note that `start` and `end` must be
  incremented before use, as they are used subsequently as the final
  argument to `subvec`, which slices a subvector up to but not
  including the final argument."
  [{:keys [est-start est-end t-res]}
  ts-start-period val-ts]
  (let [[start-idx end-idx] (date/relative-period t-res ts-start-period
                                          [est-start est-end])
        tele-start-idx (inc start-idx)
        tele-end-idx (inc end-idx)]
    (map (comp vector vec)
         (f/tele-ts tele-start-idx tele-end-idx val-ts))))

(defn dynamic-clean
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis. Note that all variable names within this
  query are TIMESERIES, not individual values."
  [est-map dynamic-src]
  (let [nodata (:nodata est-map)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?tele-ndvi ?precl]
      (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl _)
      (u/replace-from-left* nodata ?ndvi :> ?clean-ndvi)
      (telescope-ts est-map ?start ?clean-ndvi :> ?tele-ndvi)
      (:distinct false))))

(defmapop series-end
  "Return the relative index of the final element of a collection
  given a series and a starting index.

  Usage:
    (let [src [[[1 2 3]]]
          start-idx 4]
      (??<- [?end]
        (src ?series)
        (series-end ?series start-idx :> ?end))
    ;=> 6"
  [series start-idx]
  (let [length (count series)]
    (dec (+ start-idx length))))

(defn analyze-trends
  "Accepts an est-map, and sources for ndvi and rain timeseries and
  vcf values split up by pixel.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis. Note that the NDVI and PRECL variables are
  timeseries.

  Note that if any of the trends statistics functions return `nil` (e.g.
  for a singular matrix), the pixel will be dropped for that period."
  [est-map clean-src]
  (let [long-block (:long-block est-map)
        short-block (:window est-map)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?end !short !long !t-stat !break]
        (clean-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl)
        (f/shorten-ts ?ndvi ?precl :> ?short-precl)
        (a/short-stat long-block short-block ?ndvi :> !short)
        (a/long-stats ?ndvi ?short-precl :> !long !t-stat)
        (a/hansen-stat ?ndvi :> !break)
        (series-end ?ndvi ?start :> ?end)
        (:distinct false))))

(defbufferop consolidate-timeseries
  "Orders tuples by the second incoming field, inserting a supplied
   nodata value (first incoming field) where there are holes in the
   timeseries.

   Usage:
     (let [nodata -9999
           src [[1 827 1 2 3]
                [1 829 2 3 4]]]
      (??- (<- [?id ?per-ts ?f1-ts ?f2-ts ?f3-ts]
        (src ?id ?period ?f1 ?f2 ?f3)
        (consolidate-timeseries nodata ?period ?f1 ?f2 ?f3 :> ?per-ts ?f1-ts ?f2-ts ?f3-ts))))
     ;=> (([1 [827 -9999 829] [1 -9999 2] [2 -9999 3] [3 -9999 4]]))"
  [tuples]
  (let [nodata (ffirst tuples)
        field-count (count (first tuples))
        sorted-tuples (sort-by second tuples)
        idxs (vec (map second sorted-tuples))]
    [(vec (map #(mu/sparsify % nodata idxs sorted-tuples)
               (range 1 field-count)))]))

(defn trends-cleanup
  "Take a source of period-level trends output, turn each trends stat
   into a timeseries, replacing missing periods with nil.

   Usage:
     (let [src [[\"500\" 28 8 0 0 827 827 1 2 3 4]
                [\"500\" 28 8 0 0 827 829 2 3 4 5]]]
             (??- (trends-cleanup src)))
     ;=> (([\"500\" 28 8 0 0 827 829 [1 nil 2]
                                     [2 nil 3]
                                     [3 nil 4]
                                     [4 nil 5]]))
     Note how values for missing period 828 are replaced with `nil`"
  [src]
  (let [nodata nil]
    (<- [?s-res ?mh ?mv ?s ?l ?start ?max-end ?short-v ?long-v ?t-stat-v ?break-v]
        (src ?s-res ?mh ?mv ?s ?l ?start ?end !short !long !t-stat !break)
        (consolidate-timeseries nodata ?end !short !long !t-stat !break :>
                                ?end-v ?short-v ?long-v ?t-stat-v ?break-v)
        (u/filter* (partial not= nil) ?end-v :> ?end-v-good)
        (reduce max ?end-v-good :> ?max-end))))

(defn forma-tap
  "Accepts an est-map and sources for 

  Note that all values internally discuss timeseries.

  Also note that !!fire is an ungrounding variable, and triggers a
  left join with the trend result variables."
  [t-res est-map dynamic-src fire-src]
  (let [start (date/datetime->period t-res (:est-start est-map))]
    (<- [?s-res ?period ?mh ?mv ?s ?l ?forma-val]
        (fire-src ?s-res ?mh ?mv ?s ?l !!fire)
        (dynamic-src ?s-res ?mh ?mv ?s ?l _ ?end ?short ?long ?t-stat ?break)
        (schema/forma-seq !!fire ?short ?long ?t-stat ?break :> ?forma-seq)
        (p/index ?forma-seq :zero-index start :> ?period ?forma-val)
        (:distinct false))))

(defmapcatop [process-neighbors [num-neighbors]]
  "Processes all neighbors... Returns the index within the chunk, the
  value, and the aggregate of the neighbors."
  [window nodata]
  (for [[idx [val neighbors]]
        (->> (w/neighbor-scan num-neighbors window)
             (map-indexed vector))
        :when val]
    [idx val (->> neighbors
                  (apply concat)
                  (filter identity)
                  (schema/combine-neighbors nodata))]))

(defn forma-query
  "final query that walks the neighbors and spits out the values."
  [est-map forma-val-src]
  (let [{:keys [neighbors window-dims nodata]} est-map
        nodata (double nodata)
        [rows cols] window-dims
        src (p/sparse-windower forma-val-src
                               ["?sample" "?line"]
                               window-dims
                               "?forma-val"
                               nil)]
    (<- [?s-res ?period ?mod-h ?mod-v ?sample ?line ?val ?neighbor-val]
        (src ?s-res ?period ?mod-h ?mod-v ?win-col ?win-row ?window)
        (process-neighbors [neighbors] ?window nodata :> ?win-idx ?val ?neighbor-val)
        (r/tile-position cols rows ?win-col ?win-row ?win-idx :> ?sample ?line))))

(defn beta-data-prep
  "Prep data for generating betas, retaining only data for the training
   period and dropping coastal pixels and pixels with nodata values in
   thrift objects"
  [{:keys [nodata t-res est-start min-coast-dist]} dynamic-src static-src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?eco ?hansen]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco ?hansen ?coast-dist)
        (u/obj-contains-nodata? nodata ?val :> false)
        (u/obj-contains-nodata? nodata ?neighbor-val :> false)        
        (= ?pd first-idx)
        (>= ?coast-dist min-coast-dist)
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

(defmapop [apply-betas [betas]]
  [eco val neighbor-val]
  (let [beta (((comp keyword str) eco) betas)]
    (log/logistic-prob-wrap beta val neighbor-val)))

(defbufferop mk-timeseries
  "A very specific function that accepts all probabilities for a given
  pixel for all time series, sorts them by the observed period and
  then returns a single series of probabilities; used only in the
  `forma-estimate` query below."
  [tuples]
  [[(map second
         (sort-by first tuples))]])

(defn forma-estimate
  "query to end all queries: estimate the probabilities for each
  period after the training period."
  [{:keys [nodata]} beta-src dynamic-src static-src]
  (let [betas (log/beta-dict beta-src)]
    (<- [?s-res ?mod-h ?mod-v ?s ?l ?prob-series]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (u/obj-contains-nodata? nodata ?val :> false)
        (u/obj-contains-nodata? nodata ?neighbor-val :> false)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco _ _)
        (apply-betas [betas] ?eco ?val ?neighbor-val :> ?prob)
        (mk-timeseries ?pd ?prob :> ?prob-series)
        (:distinct false))))
