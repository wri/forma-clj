(ns forma.hadoop.jobs.timeseries
  (:use cascalog.api
        [forma.matrix.utils :only (sparse-expander)]
        [forma.source.tilesets :only (tile-set)])
  (:require [cascalog.ops :as c]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.hadoop.pail :as pail]
            [forma.schema :as schema]
            [forma.thrift :as thrift]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.utils :as utils]))

(defbufferop [timeseries [missing-val]]
  "Takes in a number of `<t-period, modis-chunk>` tuples,
  sorted by time period, and transposes these into (n = chunk-size)
  4-tuples, formatted as `<pixel-idx, start, end, t-series>`, where
  the `t-series` field is represented by a vector.

  Entering chunks should be sorted by `t-period` in ascending
  order. `modis-chunk` tuple fields must be vectors."
  [tuples]
  (let [[periods [val]] (apply map vector tuples)
        [fp lp] ((juxt first peek) periods)
        missing-struct (thrift/pack (repeat (thrift/count-vals val) missing-val))
        chunks (sparse-expander missing-struct tuples :start fp)
        tupleize (comp (partial vector fp lp) thrift/pack vector)]
    (->> chunks
         (map thrift/unpack)
         (apply map tupleize)
         (map-indexed cons))))

(defn form-tseries
  "Returns a predicate macro aggregator that generates a timeseries,
  given `?chunk`, `?temporal-resolution` and `?date`. Currently only
  functions when `?chunk` is a vector."
  [missing-val]
  {:pre [(number? missing-val)]}
  (<- [?temporal-res ?date ?chunk :> ?pix-idx ?t-start ?t-end ?tseries]
      (date/datetime->period ?temporal-res ?date :> ?tperiod)
      (:sort ?tperiod)
      (timeseries [missing-val] ?tperiod ?chunk :> ?pix-idx ?t-start ?t-end ?tseries)))

(defn extract-tseries
  "Given a source of chunks, this subquery extracts the proper
  position information and outputs new datachunks containing
  timeseries."
  [missing-val tile-chunk-src]
  (let [mk-tseries (form-tseries missing-val)
        val-src (<- [?name ?t-res ?date ?s-res ?h ?v ?id ?size ?data]
                    (tile-chunk-src _ ?tile-chunk)
                    (thrift/unpack ?tile-chunk :> ?name ?tile-loc ?data ?t-res ?date)
                    (thrift/unpack ?tile-loc :> ?s-res ?h ?v ?id ?size)
                     (:distinct false))
        ts-src (<- [?name ?t-res ?s-res ?h ?v ?id ?size ?pixel-idx ?ts]
                   (val-src ?name ?t-res ?date ?s-res ?h ?v ?id ?size ?val)
                   (mk-tseries ?t-res ?date ?val :> ?pixel-idx ?start ?end ?series)
                   (thrift/TimeSeries* ?start ?end ?series :> ?ts))]
    (<- [?pixel-chunk]
        (ts-src ?name ?t-res ?s-res ?h ?v ?id ?size ?pixel-idx ?ts)
        (r/tile-position ?s-res ?size ?id ?pixel-idx :> ?sample ?line)
        (thrift/ModisPixelLocation* ?s-res ?h ?v ?sample ?line :> ?pixel-loc)
        (thrift/DataChunk* ?name ?pixel-loc ?ts ?t-res :> ?pixel-chunk)
        (:distinct false))))

(def ^:dynamic *missing-val*
  -9999)

(defmain ModisTimeseries
  "Convert unordered MODIS chunks from pail into pixel-level timeseries."
  [pail-path output-path s-res t-res dataset]
  (->> (pail/split-chunk-tap pail-path [dataset (format "%s-%s" s-res t-res)])
       (extract-tseries *missing-val*)
       (?- (hfs-seqfile output-path :sinkmode :replace))))

;; #### Fire Time Series Processing

(defparallelagg merge-firevals
  "Aggregates a number of fire values by adding up the values of each
  `FireValue` property."
  :init-var    #'identity
  :combine-var #'schema/add-fires)

(defmapop running-fire-sum
  "Special case of `running-sum` for fire objects."
  [start tseries]
  (when (seq tseries)
    (->> tseries
         (reductions schema/add-fires)
         (thrift/TimeSeries* start))))

(defn aggregate-fires
  "Aggregate fires into periods given by `t-res`"
  [src t-res]
  (<- [?name ?datestring ?s-res ?h ?v ?sample ?line ?agg-fire-val]
      (src ?pixel-chunk)
      (thrift/unpack ?pixel-chunk :> ?name ?pixel-loc ?data-val _ ?date _)
      (thrift/unpack ?data-val :> ?temp-330 ?conf-50 ?bothPreds ?count)
      (thrift/FireValue* ?temp-330 ?conf-50 ?bothPreds ?count :> ?fire-val)
      (merge-firevals ?fire-val :> ?agg-fire-val)
      (date/beginning t-res ?date :> ?datestring)
      (thrift/unpack ?pixel-loc :> ?s-res ?h ?v ?sample ?line)))

(defn sum-fire-series
  "Convert fire series into a running sum of fires."
  [series-src start length t-res]
  (let [mk-fire-tseries (p/vals->sparsevec start length (thrift/FireValue* 0 0 0 0))]
    (<- [?name ?s-res ?mod-h ?mod-v ?s ?l ?fire-series]
        (series-src ?name ?datestring ?s-res ?mod-h ?mod-v ?s ?l ?tuple)
        (date/datetime->period t-res ?datestring :> ?tperiod)
        (mk-fire-tseries ?tperiod ?tuple :> _ ?tseries)
        (running-fire-sum start ?tseries :> ?fire-series)
        (:distinct true))))

(defn create-fire-series
  "Aggregates fires into running sum timeseries."
  [src t-res fires-start est-start est-end]
  (let [[start end] (map (partial date/datetime->period t-res) [fires-start est-end])
        length (inc (- end start))
        mk-fire-tseries (p/vals->sparsevec start length (thrift/FireValue* 0 0 0 0))
        series-src (aggregate-fires src t-res)
        fire-sum-src (sum-fire-series series-src start length t-res)]
    (<- [?pixel-chunk]
        (fire-sum-src ?name ?s-res ?h ?v ?sample ?line ?fire-series)
        (schema/adjust-fires est-start est-end t-res ?fire-series :> ?adjusted)
        (thrift/ModisPixelLocation* ?s-res ?h ?v ?sample ?line :> ?pixel-loc)
        (thrift/DataChunk* ?name ?pixel-loc ?adjusted t-res :> ?pixel-chunk)
        (:distinct false))))

(defn fire-query
  "Returns a source of fire timeseries data chunk objects."
  [src m-res t-res fires-start est-start est-end]
  (-> src
      (create-fire-series t-res fires-start est-start est-end)))
