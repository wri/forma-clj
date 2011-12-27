(ns forma.hadoop.jobs.timeseries
  (:use cascalog.api
        [forma.utils :only (running-sum)]
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
        [fp lp] ((juxt first peek) periods)
        missing-vec (into [] (repeat (count val) missing-val))
        chunks (sparse-expander missing-vec tuples :start fp)
        tupleize (comp (partial vector fp lp)
                       vec
                       vector)]
    (->> chunks
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
        series-src (<- [?name ?t-res ?location ?pix-idx ?timeseries]
                       (chunk-source _ ?chunk)
                       (io/extract-ts-data ?chunk
                                           :> ?name ?t-res ?date ?location ?datachunk)
                       (mk-tseries ?t-res ?date ?datachunk
                                   :> ?pix-idx ?start ?end ?tseries)
                       (schema/timeseries-value ?start ?end ?tseries :> ?timeseries))]
    (<- [?chunk]
        (series-src ?name ?t-res ?location ?pix-idx ?timeseries)
        (io/chunkloc->pixloc ?location ?pix-idx :> ?pix-location)
        (io/mk-chunk ?name ?t-res nil ?pix-location ?timeseries :> ?chunk)
        (:distinct false))))

(def ^:dynamic *missing-val* -9999)

(defn tseries-query
  [pail-path datasets]
  (-> (apply pail/split-chunk-tap pail-path datasets)
      (extract-tseries *missing-val*)))

(defmain DynamicTimeseries
  "TODO: Process a pattern, here"
  [source-pail-path ts-pail-path s-res t-res & datasets]
  (let [datasets (or datasets ["precl" "ndvi" "reli" "qual" "evi"])]
    (->> (for [dset datasets]
           [dset (format "%s-%s" s-res t-res)])
         (tseries-query source-pail-path)
         (pail/to-pail ts-pail-path))))

;; #### Fire Time Series Processing

(defaggregateop merge-firetuples
  " Aggregates a number of firetuples by adding up the values
  of each `FireTuple` property."
  ([] [0 0 0 0])
  ([state tuple] (map + state (schema/extract-fields tuple)))
  ([state] [(apply schema/fire-value state)]))


(defmapop running-fire-sum
  "Special case of `running-sum` for `FireTuple` thrift objects."
  [start tseries]
  (let [empty (schema/fire-value 0 0 0 0)]
    (->> tseries
         (running-sum [] empty schema/add-fires)
         (schema/timeseries-value start))))

(defn aggregate-fires
  "Converts the datestring into a time period based on the supplied
  temporal resolution."
  [src t-res]
  (let [mash (c/juxt #'io/extract-dataset
                     #'io/get-location-property
                     #'io/extract-date
                     (c/comp merge-firetuples #'io/extract-chunk-value))]
    (<- [?name ?datestring ?location ?tuple]
        (src _ ?chunk)
        (mash ?chunk :> ?name ?location ?date ?tuple)
        (date/beginning t-res ?date :> ?datestring)
        (:distinct false))))

(defn create-fire-series
  "Aggregates fires into timeseries."
  [src t-res start end]
  (let [[start end]     (map (partial date/datetime->period t-res) [start end])
        length          (inc (- end start))
        mk-fire-tseries (p/vals->sparsevec start length (schema/fire-value 0 0 0 0))
        series-src      (aggregate-fires src t-res)
        query (<- [?name ?location ?fire-series]
                  (series-src ?name ?datestring ?location ?tuple)
                  (date/datetime->period t-res ?datestring :> ?tperiod)
                  (mk-fire-tseries ?tperiod ?tuple :> _ ?tseries)
                  (running-fire-sum start ?tseries :> ?fire-series))]
    (<- [?chunk]
        (query ?name ?location ?fire-series)
        (io/mk-chunk ?name t-res nil ?location ?fire-series :> ?chunk))))

(defn fire-query
  "Returns a source of fire timeseries data chunk objects."
  [source-pail-path t-res start end tile-seq]
  (let [tap (apply pail/split-chunk-tap
                   source-pail-path
                   (for [tile (apply tile-set tile-seq)]
                     ["fire" "1000-01" (apply r/hv->tilestring tile)]))]
    (create-fire-series tap t-res start end)))
