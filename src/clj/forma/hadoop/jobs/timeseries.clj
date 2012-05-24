(ns forma.hadoop.jobs.timeseries
  (:use cascalog.api
        [forma.matrix.utils :only (sparse-expander)]
        [forma.source.tilesets :only (tile-set)])
  (:require [cascalog.ops :as c]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.hadoop.pail :as pail]
            [forma.schema :as schema]
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
        [fp lp]        ((juxt first peek) periods)
        missing-struct (schema/to-struct (repeat (schema/count-vals val) missing-val))
        chunks         (sparse-expander missing-struct tuples :start fp)
        tupleize       (comp (partial vector fp lp)
                             schema/to-struct
                             vector)]
    (->> chunks
         (map schema/get-vals)
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
  [chunk-source missing-val]
  (let [mk-tseries (form-tseries missing-val)
        data-src   (<- [?name ?t-res ?date ?s-res ?mod-h ?mod-v ?chunk-idx ?size ?datachunk]
                       (chunk-source _ ?chunk)
                       (schema/unpack-chunk-val ?chunk :> ?name ?t-res ?date ?location ?datachunk)
                       (schema/unpack-chunk-location ?location :> ?s-res ?mod-h ?mod-v ?chunk-idx ?size)
                       (:distinct false))
        series-src (<- [?name ?t-res ?s-res ?mod-h ?mod-v ?chunk-idx ?size ?pix-idx ?timeseries]
                       (data-src ?name ?t-res ?date ?s-res ?mod-h ?mod-v ?chunk-idx ?size ?datachunk)
                       (mk-tseries ?t-res ?date ?datachunk :> ?pix-idx ?start ?end ?tseries)
                       (schema/mk-array-value ?tseries :> ?array-val)
                       (schema/timeseries-value ?start ?end ?array-val :> ?timeseries))]
    (<- [?chunk]
        (series-src ?name ?t-res ?s-res ?mod-h ?mod-v ?chunk-idx ?size ?pix-idx ?timeseries)
        (r/tile-position ?s-res ?size ?chunk-idx ?pix-idx :> ?sample ?line)
        (schema/pixel-location ?s-res ?mod-h ?mod-v ?sample ?line :> ?pix-location)
        (schema/mk-data-value ?timeseries :> ?data-val)
        (schema/chunk-value ?name ?t-res nil ?pix-location ?data-val :> ?chunk)
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
         (schema/timeseries-value start))))

(defn aggregate-fires
  "Converts the datestring into a time period based on the supplied
  temporal resolution."
  [src t-res]
  (<- [?name ?datestring ?s-res ?mod-h ?mod-v ?s ?l ?tuple]
      (src _ ?chunk)
      (schema/unpack-chunk-val ?chunk :> ?name _ ?date ?location ?val)
      (merge-firevals ?val :> ?tuple)
      (date/beginning t-res ?date :> ?datestring)
      (schema/unpack-pixel-location ?location :> ?s-res ?mod-h ?mod-v ?s ?l)
      (:distinct false)))

(defn create-fire-series
  "Aggregates fires into timeseries."
  [src t-res start end]
  (let [[start end]     (map (partial date/datetime->period t-res) [start end])
        length          (inc (- end start))
        mk-fire-tseries (p/vals->sparsevec start length (schema/fire-value 0 0 0 0))
        series-src      (aggregate-fires src t-res)
        query (<- [?name ?s-res ?mod-h ?mod-v ?s ?l ?fire-series]
                  (series-src ?name ?datestring ?s-res ?mod-h ?mod-v ?s ?l ?tuple)
                  (date/datetime->period t-res ?datestring :> ?tperiod)
                  (mk-fire-tseries ?tperiod ?tuple :> _ ?tseries)
                  (running-fire-sum start ?tseries :> ?fire-series))]
    (<- [?chunk]
        (query ?name ?s-res ?mod-h ?mod-v ?s ?l ?fire-series)
        (schema/pixel-location ?s-res ?mod-h ?mod-v ?s ?l :> ?location)
        (schema/mk-data-value ?fire-series :> ?data-val)
        (schema/chunk-value ?name t-res nil ?location ?data-val :> ?chunk)
        (:distinct false))))

(defn fire-query
  "Returns a source of fire timeseries data chunk objects."
  [source-pail-path t-res start end]
  (-> source-pail-path
      (pail/split-chunk-tap ["fire" "1000-01"])
      (create-fire-series t-res start end)))
