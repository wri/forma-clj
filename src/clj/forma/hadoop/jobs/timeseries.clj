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
            [forma.hadoop.predicate :as p]))

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
  (<- [?temporal-res ?date ?chunk :> ?pix-idx ?t-start ?t-end ?tseries]
      (date/datetime->period ?temporal-res ?date :> ?tperiod)
      (:sort ?tperiod)
      (timeseries [missing-val] ?tperiod ?chunk :> ?pix-idx ?t-start ?t-end ?tseries)))

(defn extract-tseries
  "Given a source of chunks, this subquery extracts the proper
  position information and outputs new datachunks containing
  timeseries."
  [tile-chunk-src missing-val]
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
        (thrift/DataChunk* ?name ?tile-loc ?ts ?t-res :> ?pixel-chunk)
        (:distinct false))))

(def ^:dynamic *missing-val*
  -9999)

(defn tseries-query
  [pail-path datasets]
  (-> (apply pail/split-chunk-tap pail-path datasets)
      (extract-tseries *missing-val*)))

(defmain DynamicTimeseries
  "TODO: Process a pattern, here"
  [source-pail-path ts-pail-path s-res t-res datasets]
  {:pre [(vector? (read-string datasets))]}
  (->> (for [dset  (read-string datasets)]
         [dset (format "%s-%s" s-res t-res)])
       (tseries-query source-pail-path)
       (pail/to-pail ts-pail-path)))

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
         (schema/fire-series start))))

(defn aggregate-fires
  "Converts the datestring into a time period based on the supplied
  temporal resolution."
  [src t-res]
  (<- [?name ?datestring ?s-res ?h ?v ?sample ?line ?agg-fire-val]
      (src _ ?pixel-chunk)
      (thrift/unpack ?pixel-chunk :> ?name ?pixel-loc ?data _ ?date)
      (thrift/unpack ?data :> ?temp330 ?conf50 ?bothPreds ?count)
      (thrift/FireValue* ?temp330 ?conf50 ?bothPreds ?count :> ?fire-val)
      (merge-firevals ?fire-val :> ?agg-fire-val)
      (date/beginning t-res ?date :> ?datestring)
      (thrift/unpack ?pixel-loc :> ?s-res ?h ?v ?sample ?line)))

(defn create-fire-series
  "Aggregates fires into timeseries."
  [src t-res start end]
  (let [[start end] (map (partial date/datetime->period t-res) [start end])
        length (inc (- end start))
        mk-fire-tseries (p/vals->sparsevec start length (thrift/FireValue* 0 0 0 0))
        series-src (aggregate-fires src t-res)
        query (<- [?name ?s-res ?mod-h ?mod-v ?s ?l ?fire-series]
                  (series-src ?name ?datestring ?s-res ?mod-h ?mod-v ?s ?l ?tuple)
                  (date/datetime->period t-res ?datestring :> ?tperiod)
                  (mk-fire-tseries ?tperiod ?tuple :> _ ?tseries)
                  (running-fire-sum start ?tseries :> ?fire-series)
                  (:trap (hfs-textline "s3n://formareset/fire-trap" :sinkmode :replace))
                  (:distinct true))]
    (<- [?pixel-chunk]
        (query ?name ?s-res ?h ?v ?sample ?line ?fire-series)
        (thrift/ModisPixelLocation* ?s-res ?h ?v ?sample ?line :> ?pixel-loc)
        (thrift/DataChunk* ?name ?pixel-loc ?fire-series t-res :> ?pixel-chunk)
        (:distinct false))))

(defn fire-query
  "Returns a source of fire timeseries data chunk objects."
  [source-pail-path t-res start end]
  (-> source-pail-path
      (pail/split-chunk-tap ["fire" "1000-01"])
      (create-fire-series t-res start end)))

