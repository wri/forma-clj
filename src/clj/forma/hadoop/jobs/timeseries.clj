(ns forma.hadoop.jobs.timeseries
  (:use cascalog.api
        [forma.utils :only (defjob)]
        [forma.matrix.utils :only (sparse-expander)])
  (:require [cascalog.ops :as c]
            [forma.date-time :as date]
            [forma.hadoop.pail :as pail]
            [forma.utils :as utils]
            [forma.source.modis :as m]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]))

(defbufferop [timeseries [missing-val]]
  "Takes in a number of `<t-period, modis-chunk>` tuples,
  sorted by time period, and transposes these into (n = chunk-size)
  4-tuples, formatted as <pixel-idx, t-start, t-end, t-series>, where
  the `t-series` field is represented by an instance of
  `forma.schema.DoubleArray`.

  Entering chunks should be sorted by `t-period` in ascending
  order. `modis-chunk` tuple fields must be vectors or instances of
  `forma.schema.DoubleArray` or `forma.schema.IntArray`, as dictated
  by the Thriftable interface in `forma.hadoop.io`."
  [tuples]
  (let [[periods [val]] (apply map vector tuples)
        [fp lp] ((juxt first peek) periods)
        missing-struct (io/to-struct (repeat (io/count-vals val) missing-val))
        chunks (sparse-expander missing-struct tuples :start fp)
        tupleize (comp (partial vector fp lp)
                       io/to-struct
                       vector)]
    (->> chunks
         (map io/get-vals)
         (apply map tupleize)
         (map-indexed cons))))

(defn form-tseries
  "Returns a predicate macro aggregator that generates a timeseries,
  given `?chunk`, `?temporal-resolution` and `?date`. Currently only
  functions when `?chunk` is an instance of
  `forma.schema.DoubleArray`."
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
        series-src (<- [?name ?location ?pix-idx ?series-struct]
                       (chunk-source _ ?chunk)
                       (io/extract-location ?chunk :> ?location)
                       (io/extract-timeseries-data ?chunk :> ?name ?t-res ?date ?datachunk)
                       (mk-tseries ?t-res ?date ?datachunk :> ?pix-idx ?start ?end ?tseries)
                       (io/mk-array-value ?tseries :> ?series-val)
                       (io/timeseries-value ?start ?end ?series-val :> ?series-struct))]
    (<- [?final-chunk]
        (chunk-source _ ?chunk)
        (io/extract-location ?chunk :> ?location)
        (series-src ?name ?location ?pix-idx ?series-struct)
        (io/chunkloc->pixloc ?location ?pix-idx :> ?pix-location)
        (io/massage-ts-chunk ?chunk ?series-struct ?pix-location :> ?final-chunk))))

(def *missing-val* -9999)

(defn tseries-query [pail-path]
  (-> pail-path
      (pail/split-chunk-tap ["ndvi"] ["precl"] ["reli"] ["qual"] ["evi"])
      (extract-tseries *missing-val*)))

(defjob DynamicTimeseries
  "TODO: Process a pattern, here."
  [source-pail-path ts-pail-path]
  (->> (tseries-query source-pail-path)
       (pail/to-pail ts-pail-path)))

;; #### Fire Time Series Processing

(defaggregateop merge-firetuples
  " Aggregates a number of firetuples by adding up the values
  of each `FireTuple` property."
  ([] [0 0 0 0])
  ([state tuple] (map + state (io/extract-fields tuple)))
  ([state] [(apply io/fire-tuple state)]))

;; Special case of `running-sum` for `FireTuple` thrift objects.
(defmapop running-fire-sum
  [tseries]
  (let [empty (io/fire-tuple 0 0 0 0)]
    (->> tseries
         (utils/running-sum [] empty io/add-fires)
         io/fire-series)))

(defn old-aggregate-fires
  "Converts the datestring into a time period based on the supplied
  temporal resolution."
  [t-res src]
  (<- [?dataset ?m-res ?new-t-res ?tilestring ?tperiod ?sample ?line ?newtuple]
      (p/add-fields t-res :> ?new-t-res)
      (src ?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple)
      (date/datetime->period ?new-t-res ?datestring :> ?tperiod)
      (merge-firetuples ?tuple :> ?newtuple)))

(defn old-fire-series
  "Aggregates fires into timeseries."
  [t-res start end src]
  (let [[start end] (map (partial date/datetime->period t-res) [start end])
        length (-> end (- start) inc)
        mk-fire-tseries (p/vals->sparsevec start length (io/fire-tuple 0 0 0 0))]
    (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?t-start ?t-end ?ct-series]
        (src ?dataset ?m-res ?t-res ?tilestring ?tperiod ?sample ?line ?tuple)
        (m/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
        (mk-fire-tseries ?tperiod ?tuple :> _ ?tseries)
        (p/add-fields start end :> ?t-start ?t-end)
        (running-fire-sum ?tseries :> ?ct-series))))

(defn old-fire-query
  [t-res start end chunk-path]
  (->> (hfs-seqfile chunk-path)
       (old-aggregate-fires t-res)
       (old-fire-series t-res start end)))

(defjob OldFireTimeseries
  [t-res start end chunk-path tseries-path]
  (?- (hfs-seqfile tseries-path)
      (old-fire-query t-res start end chunk-path)))

;; New Stuff!

(defn update-chunk
  [chunk t-res date firetuple]
  (->  (io/set-date date)
       (io/set-temporal-res t-res)
       (io/swap-data (io/mk-data-value firetuple))))

(defn aggregate-fires
  "Converts the datestring into a time period based on the supplied
  temporal resolution."
  [src t-res]
  (<- [?datestring ?location ?final-chunk]
      (src _ ?chunk)
      (io/extract-location ?chunk :> ?location)
      ((c/comp #'date/beginning #'io/extract-date) ?chunk :> ?datestring)
      ((c/comp merge-firetuples #'io/extract-chunk-value) ?chunk :> ?tuple)
      (update-chunk ?chunk t-res ?datestring ?tuple :> ?final-chunk)))

;; TODO: Update
(defn fire-series
  "Aggregates fires into timeseries."
  [src t-res start end]
  (let [[start end] (map (partial date/datetime->period t-res) [start end])
        length (-> end (- start) inc)
        mk-fire-tseries (p/vals->sparsevec start length (io/fire-tuple 0 0 0 0))]
    (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?t-start ?t-end ?ct-series]

        (src ?dataset ?m-res ?t-res ?tilestring ?tperiod ?sample ?line ?tuple)

        (src ?datestring ?location ?final-chunk)

        (m/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
        (mk-fire-tseries ?tperiod ?tuple :> _ ?tseries)
        (p/add-fields start end :> ?t-start ?t-end)
        (running-fire-sum ?tseries :> ?ct-series))))

(defn fire-query
  [pail-path t-res start end]
  (-> pail-path
      (pail/split-chunk-tap ["fire"])
      (aggregate-fires t-res)
      (fire-series t-res start end)))

(defjob FireTimeseries
  [t-res start end source-pail-path ts-pail-path]
  (-> source-pail-path
      (fire-query t-res start end)
      (pail/to-pail ts-pail-path)))
