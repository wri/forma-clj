(ns forma.hadoop.jobs.forma
  (:use cascalog.api
        [forma.hadoop.pail :only (split-chunk-tap)]
        [forma.source.ecoregion :only (get-ecoregion get-super-ecoregion)])
  (:require [cascalog.ops :as c]
            [forma.matrix.walk :as w]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.thrift :as thrift]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a]
            [forma.ops.classify :as classify]
            [forma.trends.filter :as f]
            [forma.utils :as u]
            [forma.source.humidtropics :as humid]
            [forma.matrix.utils :as mu]
            [forma.trends.stretch :as stretch]))

(defn static-tap
  "Accepts a source of DataChunks, and returns a new query with all
   relevant spatial information (resolution, MODIS tile info, pixel
   sample & line) plus the actual, unpacked data value."
  [chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (chunk-src _ ?chunk)
      (thrift/unpack ?chunk :> _ ?loc ?data _ _ _)
      (thrift/get-field-value ?data :> ?val)
      (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn consolidate-static
  "Due to an issue with Pail, we consolidate separate sequence files
   of static data with one big join.

   We'll screen out border pixels later - doing it here will remove non-water
   but nearly water pixels before they can be included as neighbors"
  [vcf-limit vcf-src gadm-src hansen-src ecoid-src border-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?vcf ?gadm ?ecoid ?hansen ?coast-dist]
      (vcf-src    ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
      (hansen-src ?s-res ?mod-h ?mod-v ?sample ?line ?hansen)
      (ecoid-src  ?s-res ?mod-h ?mod-v ?sample ?line ?ecoid)
      (gadm-src   ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
      (border-src ?s-res ?mod-h ?mod-v ?sample ?line ?coast-dist)
      (>= ?vcf vcf-limit)
      (humid/in-humid-tropics? ?ecoid)))

(defn screen-by-tileset
  [src tile-set]
  (<- [?pail-path ?pixel-chunk]
      (src ?pail-path ?pixel-chunk)
      (thrift/unpack ?pixel-chunk :> _ ?pixel-loc _ _ _ _)
      (thrift/unpack ?pixel-loc :> _ ?h ?v _ _)
      (u/within-tileset? tile-set ?h ?v)))

(defn adjust-precl
  "Given a base temporal resolution, a target temporal resolution and a
   source of rain data, stretches rain to new resolution and rounds it."
  [base-t-res target-t-res src]
  (if (= target-t-res base-t-res)
      src
      (<- [?s-res ?mod-h ?mod-v ?sample ?line ?new-start-idx ?rounded-series]
          (src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?ts)
          (thrift/TimeSeries* ?start-idx ?ts :> ?ts-obj)
          (stretch/ts-expander base-t-res target-t-res ?ts-obj :> ?expanded-ts-obj)
          (thrift/unpack ?expanded-ts-obj :> ?new-start-idx _ ?arr-val)
          (thrift/unpack* ?arr-val :> ?expanded-series)
          (u/map-round* ?expanded-series :> ?rounded-series)
          (:distinct false))))

(defn fire-tap
  "Accepts an est-map and a query source of fire timeseries. Note that
  this won't work, pulling directly from the pail!"
  [fire-src]
  (<- [?s-res ?h ?v ?sample ?line ?fire-series]
      (fire-src ?fire-pixel)
      (thrift/unpack ?fire-pixel :> _ ?pixel-loc ?ts _ _ _)
      (thrift/unpack ?ts :> ?start _ ?array-val)
      (thrift/unpack* ?array-val :> ?fire-vec)
      (thrift/TimeSeries* ?start ?fire-vec :> ?fire-series)
      (thrift/unpack ?pixel-loc :> ?s-res ?h ?v ?sample ?line)))

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
      (chunk-src ?ts-chunk)
      (static-src ?s-res ?mod-h ?mod-v ?sample ?line ?vcf _ _ _ _)

      ;; unpack ts object
      (thrift/unpack ?ts-chunk :> _ ?ts-loc ?ts-data _ _ _)
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
  "Filters out all NDVI pixels where timeseries is all -3000s. Trims
   ndvi and rain timeseries so that they are the same length,
   and replaces any nodata values with the value to their left. Leaves
   nodata value if at the start of the timeseries"
  [{:keys [t-res nodata est-start]} ndvi-src rain-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?ndvi-ts ?precl-ts]
      (ndvi-src ?s-res ?mod-h ?mod-v ?sample ?line ?n-start ?ndvi)
      (rain-src ?s-res ?mod-h ?mod-v ?sample ?line ?p-start ?precl)
      (training-3000s? t-res ?n-start est-start ?ndvi :> false)
      (u/replace-from-left* nodata ?ndvi :default nodata :all-types true :> ?ndvi-clean)
      (u/replace-from-left* nodata ?precl :default nodata :all-types true :> ?precl-clean)
      (schema/adjust ?p-start ?precl-clean
                     ?n-start ?ndvi-clean
                     :> ?start-idx ?precl-ts ?ndvi-ts)))

(defn series-end
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

(defn calculate-trends
  "Calculates all trend statistics for a timeseries, also returns the
   period index of the last element of the series. No longer calculates
   short trend statistic."
  [window long-block start-idx ts rain-ts]
  (let [end-idx (series-end ts start-idx)
        short-rain (first (f/shorten-ts ts rain-ts))]
    (flatten [end-idx
;;              (a/short-stat long-block window ts)
              (a/long-stats ts short-rain)
              (a/hansen-stat ts)])))

(defn expand-rain [rain-ts long-teleseries]
  (let [base-vec (vec rain-ts)
        size (count base-vec)
        should-be (count long-teleseries)]
    (if (> should-be size)
      (vec (concat base-vec (repeat (- should-be size) (peek base-vec))))
      base-vec)))

(defn telescoping-trends
  "Maps `calculate-trends` onto each part of an ever-lengthening subset
   of the input timeseries, from `est-start` to `est-end`. Returns
   timeseries for each of the trend statistics."
  [{:keys [est-start est-end t-res window long-block]}
   ts-start-period val-ts rain-ts]
  (let [[start-idx end-idx] (date/relative-period t-res ts-start-period
                                                  [est-start est-end])
        tele-start-idx (inc start-idx)
        tele-end-idx (inc end-idx)
        tele-series (f/tele-ts tele-start-idx tele-end-idx val-ts)
        rain-ts (expand-rain rain-ts (last tele-series))
        ;;takes the last telescoping series (the longest one), and computes
        ;;the windowed trends, and then the moving averages. Then does
        ;;reductions min on the moving averages. We want the same
        ;;number of short-stats as there are tele-series (remember
        ;;each short stat is the largest short term drop observed over
        ;;that series).
        short-stats (take-last (count tele-series)
                               (a/short-stat-all long-block window (last tele-series)))
        calculate #(calculate-trends window long-block
                               ts-start-period % rain-ts)
        all-trends-but-short (mu/transpose
                              (map calculate tele-series))]
    (into [(first all-trends-but-short) (vec short-stats)]
          (take-last 3 all-trends-but-short))))

(defmapcatop telescoping-trends-wrapper
  "Wrapper for `telescoping-trends` makes it easier to test that
   function outside of the Cascalog context."
  [est-map ts-start-period val-ts rain-ts]
  [(telescoping-trends est-map ts-start-period val-ts rain-ts)])

(defn analyze-trends
  "Accepts an est-map and a source for both ndvi and rain timeseries.
  Does some simple cleaining of `nodata` values and hands off
  telescoping to `telescoping-trends`. The trend statistics are returned
  as timeseries.

  We break this apart from dynamic-filter to force the filtering to
  occur before the analysis. Note that the NDVI and PRECL variables are
  timeseries.

  Note that if the trend statistics are `nil` (e.g. for a singular
  matrix), the pixel will _not_ be dropped for that period."
  [est-map dynamic-src]
  (let [nodata (:nodata est-map)
        long-block (:long-block est-map)
        short-block (:window est-map)
        t-res (:t-res est-map)
        start-idx (date/datetime->period t-res (:est-start est-map))]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?end-idx ?short ?long ?t-stat ?break]
        (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl)
        (u/replace-from-left* nodata ?ndvi :all-types true :> ?clean-ndvi)
        (telescoping-trends-wrapper est-map ?start ?clean-ndvi ?precl
                                    :> ?end-idxs ?short ?long ?t-stat ?break)
        (p/add-fields start-idx :> ?start-idx)
        (reduce max ?end-idxs :> ?end-idx)
        (:distinct false))))

(defn trends->datachunks
  "Query converts trends output to DataChunk thrift objects suitable for pail.

   `nil` values in the stats time series cannot be used with a FormaValue (which only
   accepts float values), so they must be replaced. They are replaced with nodata values,
   currently hardcoded as -9999.0."
  [est-map trends-src & {:keys [pedigree] :or {pedigree (thrift/epoch)}}] ; default to now
  (let [data-name "trends"
        nodata (:nodata est-map)
        t-res (:t-res est-map)]
    (<- [?dc]
        (trends-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?end ?short ?long ?t-stat ?break)
        ((c/each #'u/nils->neg9999*) ?short ?long ?t-stat ?break :> ?short-n ?long-n ?t-stat-n ?break-n)
        (schema/series->forma-values nil ?short-n ?long-n ?t-stat-n ?break-n :> ?forma-vals)
        (thrift/TimeSeries* ?start ?end ?forma-vals :> ?fv-series)
        (thrift/ModisPixelLocation* ?s-res ?mod-h ?mod-v ?sample ?line :> ?loc)
        (thrift/DataChunk* data-name ?loc ?fv-series t-res :pedigree pedigree :> ?dc))))

(defn merge-sorted
  "Given tuples of time series sorted by Pedigree (or any other index),
   merges the (possibly overlapping) time series. Conflicts between
   overlapping series are resolved by retaining the value from most
   recent version of the time series."
  [t-res nodata field-n sorted &
   {:keys [consecutive] :or {consecutive false}}]
  (let [start-keys (map second sorted)
        series (map #(nth % field-n) sorted)]
    (->> (map #(date/ts-vec->ts-map %1 t-res %2) start-keys series)
         (apply merge)
         (#(date/ts-map->ts-vec t-res % nodata :consecutive consecutive)))))

(defn merge-series
  "Sorts time series by pedigree (creation date), then merges them
  into a master time series using `merge-sorted`.

  The fields in `tuples` must be pedigree, start keys then one or more
  time series fields."
  [t-res nodata tuples & {:keys [consecutive] :or {consecutive false}}]
   (let [rng (range 2 (count (first tuples)))
         sorted (sort-by first tuples)
         start-keys (map second sorted)
         start-idx (apply min (map (partial date/key->period t-res) start-keys))]
     (->> (for [field-n rng]
            (merge-sorted t-res nodata field-n sorted :consecutive consecutive))
          (vec)
          (into [start-idx])
          (vector))))

(defbufferop [merge-series-wrapper [t-res nodata]]
  "defbufferop wrapper for `merge-series`."
  [tuples]
  (let [consecutive true]
    (merge-series t-res nodata tuples :consecutive consecutive)))

(defn array-val->series
  "Given an ArrayValue of FormaValues (the product of unpacking the
   TimeSeries object inside a DataChunk), unpack the ArrayValue and
   return series of each component of a FormaValue - fires, shorts,
   longs, t-stats and breaks."
  [array-val] {:pre [(= forma.schema.ArrayValue (type array-val))
                     (= forma.schema.FormaValue (type (first (thrift/unpack array-val))))]}
  (let [forma-vals (thrift/unpack array-val)]
    (apply map vector (map thrift/unpack forma-vals))))

(def unpack-ts-for-merge
  (<- [?dc :> ?s-res ?mod-h ?mod-v ?sample ?line ?start ?end ?array-val ?created]
      (thrift/unpack ?dc :> _ ?loc ?data ?t-res _ ?pedigree)
      (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
      (thrift/unpack ?pedigree :> ?created)
      (thrift/unpack ?data :> ?start ?end ?array-val)))

(defn trends-datachunks->series
  [est-map pail-src]
  (let [data-name "trends"
        nodata (:nodata est-map)
        t-res (:t-res est-map)
        res-str (format "%s-%s" (:s-res est-map) (:t-res est-map))]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-final
         ?short-final ?long-final ?t-stat-final ?break-final]
        (pail-src _ ?dc)
        (thrift/unpack ?dc :> _ ?loc ?data ?t-res _ ?pedigree)
        (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
        (thrift/unpack ?pedigree :> ?created)
        (thrift/unpack ?data :> ?start ?end ?array-val)
        (array-val->series ?array-val :> _ ?short ?long ?t-stat ?break)
        (date/period->key ?t-res ?start :> ?start-key)
        (merge-series-wrapper [t-res nodata] ?created ?start-key ?short
                              ?long ?t-stat ?break :> ?start-final ?short-final
                              ?long-final ?t-stat-final ?break-final))))

(defn forma-tap
  "Accepts an est-map and sources for dynamic and fires data,
  spits out FormaValues for each period.

  Note that all values internally discuss timeseries, and that !!fire
  is an ungrounding variable. This triggers a left join with the trend
  result variables, even when there are no fires for a particular pixel.

  Also note that the various time series are expected to be long - for
  `dynamic-src`, they should start at the end of the training
  period (e.g. 2005-12-19). For fires, they should be full time series
  back to the early 2000s and run longer than the trends series. The
  fire series will be truncated to match the trends
  series. Pre-truncated fire series will not work as the indexing will
  be off."
  [{:keys [t-res est-start est-end nodata]} dynamic-src fire-src]
  (let [start (date/datetime->period t-res est-start)
        end (date/datetime->period t-res est-end)]
    (<- [?s-res ?period ?mod-h ?mod-v ?sample ?line ?forma-val] ;; ?forma-val ?period
        (fire-src ?s-res ?mod-h ?mod-v ?sample ?line !!fire-series)
        (dynamic-src ?s-res ?mod-h ?mod-v ?sample ?line
                     ?start-idx ?short-s ?long-s ?t-stat-s ?break-s)
        (schema/adjust-fires-simple ?start-idx ?short-s !!fire-series :> !adjusted-fires)
        (schema/forma-seq nodata !adjusted-fires ?short-s ?long-s ?t-stat-s ?break-s :> ?forma-seq)
        (p/index ?forma-seq :zero-index ?start-idx :> ?period ?forma-val)
        (<= ?period end)
        (>= ?period start)
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

(defn neighbor-query
  "final query that walks the neighbors and spits out the values."
  [{:keys [neighbors window-dims nodata]} forma-val-src]
  (let [[rows cols] window-dims
        src (p/sparse-windower forma-val-src
                               ["?sample" "?line"]
                               window-dims
                               "?forma-val"
                               nil)]
    (<- [?s-res ?period ?mod-h ?mod-v ?sample ?line ?val ?neighbor-val]
        (src ?s-res ?period ?mod-h ?mod-v ?win-col ?win-row ?window)
        (process-neighbors [neighbors] ?window nodata :> ?win-idx ?val ?neighbor-val)
        (r/tile-position cols rows ?win-col ?win-row ?win-idx :> ?sample ?line))))

(defmapcatop eco-and-super
  "Wrapper for `get-ecoregion` returns an ecoid and its associated
  super-ecoregion.

  This is needed when using super-ecoregions to correct estimation
  because 1) pixels that fall into sparsely cleared ecoregions need to
  be associated with a super-ecoregion for estimation, and 2) pixels
  that fall into non-sparsely cleared ecoregions don't need the
  super-ecoregion when the betas are applied for classification, but
  need to be included in the estimation of the betas for the
  super-ecoregion.

  For example, a pixel in a heavily cleared part of north-eastern
  Brazil could be part of a super-ecoregion that is used to classify a
  pixel in a sparsely-cleared section of Guyana"
  [ecoid]
  [ecoid (get-super-ecoregion ecoid)])

(defn beta-data-prep
  "Prep data for generating betas, retaining only data for the training
   period and dropping coastal pixels and pixels with nodata values in
   thrift objects"
  [{:keys [nodata t-res est-start min-coast-dist]} dynamic-src static-src
   & {:keys [super-ecoregions] :or {super-ecoregions false}}]
  (let [first-idx (date/datetime->period t-res est-start)
        clean-data (<- [?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?ecoid ?hansen]
                       (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
                       (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?ecoid ?hansen ?coast-dist)
                       (thrift/obj-contains-nodata? nodata ?val :> false)
                       (thrift/obj-contains-nodata? nodata ?neighbor-val :> false)
                       (= ?pd first-idx)
                       (>= ?coast-dist min-coast-dist)
                       (:distinct false))]
    (if (not super-ecoregions)
      clean-data
      (<- [?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?ecoregion ?hansen]
          (clean-data ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?ecoid ?hansen)
          (eco-and-super ?ecoid :> ?ecoregion)))))

(defn beta-gen
  "query to return the beta vector associated with each ecoregion"
  [{:keys [t-res est-start ridge-const convergence-thresh max-iterations]} src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?ecoregion ?beta]
        (src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?ecoregion ?hansen)
        (classify/logistic-beta-wrap
         [ridge-const convergence-thresh max-iterations]
         ?hansen ?val ?neighbor-val :> ?beta)
        (:distinct false))))

(defmapop [apply-betas [betas]]
  [eco val neighbor-val]
  (let [beta (((comp keyword str) eco) betas)]
    (classify/logistic-prob-wrap beta val neighbor-val)))

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

(defn forma-estimate
  "query to end all queries: estimate the probabilities for each
  period after the training period."
  [{:keys [nodata]} beta-src dynamic-src static-src
   & {:keys [super-ecoregions] :or {super-ecoregions false}}]
  (let [betas (classify/beta-dict beta-src)]
    (<- [?s-res ?mod-h ?mod-v ?s ?l ?start-idx ?prob-series]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (thrift/obj-contains-nodata? nodata ?val :> false)
        (thrift/obj-contains-nodata? nodata ?neighbor-val :> false)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?ecoregion _ _)
        (get-ecoregion ?ecoregion :super-ecoregions super-ecoregions :> ?final-eco)
        (apply-betas [betas] ?final-eco ?val ?neighbor-val :> ?prob)
        (consolidate-timeseries nodata ?pd ?prob :> ?pd-series ?prob-series)
        (first ?pd-series :> ?start-idx)
        (:distinct false))))

(defn probs->datachunks
  "Query converts probability series into TimeSeries DataChunk objects
  suitable for pail."
  [est-map prob-src & {:keys [pedigree] :or {pedigree (thrift/epoch)}}]
  (let [data-name "forma"
        nodata (:nodata est-map)
        t-res (:t-res est-map)]
    (<- [?dc]
        (prob-src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?prob-series)
        (u/replace-all* 'NA nodata ?prob-series :> ?no-na-probs)
        (thrift/TimeSeries* ?start-idx ?no-na-probs :> ?ts)
        (thrift/ModisPixelLocation* ?s-res ?mod-h ?mod-v ?sample ?line :> ?loc)
        (thrift/DataChunk* data-name ?loc ?ts t-res :pedigree pedigree :> ?dc))))

(defn probs-datachunks->series
  [est-map pail-src]
  (let [data-name "forma"
        nodata (:nodata est-map)
        t-res (:t-res est-map)
        res-str (format "%s-%s" (:s-res est-map) (:t-res est-map))]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-final ?merged-series]
        (pail-src _ ?dc)
        (thrift/unpack ?dc :> _ ?loc ?data ?t-res _ ?pedigree)
        (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
        (thrift/unpack ?pedigree :> ?created)
        (thrift/unpack ?data :> ?start ?end ?array-val)
        (thrift/unpack* ?array-val :> ?prob-series)
        (date/period->key ?t-res ?start :> ?start-key)
        (merge-series-wrapper [t-res nodata] ?created ?start-key ?prob-series
                              :> ?start-final ?merged-series))))

(defn probs-gadm2
  [probs-src gadm2-src static-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-final ?merged-series ?gadm2 ?ecoid]
      (probs-src ?s-res ?mod-h ?mod-v ?sample ?line ?start-final ?merged-series)
      (gadm2-src ?s-res ?mod-h ?mod-v ?sample ?line ?gadm2)
      (static-src ?s-res ?mod-h ?mod-v ?sample ?line _ _ ?ecoid _ _)))
