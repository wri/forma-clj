(ns forma.source.fire
  (:use cascalog.api
        [forma.date-time :only (datetime->period)]
        [clojure.string :only (split)]
        [forma.source.modis :only (latlon->modis
                                   hv->tilestring)])
  (:require [forma.hadoop.predicate :as p])
  (:import [forma.schema FireTuple TimeSeries]
           [java.util ArrayList]))

(defn fire-tuple
  "Clojure wrapper around the java `FireTuple` constructor."
  [t-above-330 c-above-50 both-preds count]
  (FireTuple. t-above-330 c-above-50 both-preds count))

(defn extract-fields
  [f-tuple]
  [(.temp330 f-tuple)
   (.conf50 f-tuple)
   (.bothPreds f-tuple)
   (.count f-tuple)])

(defn add-fires
  "Adds together two FireTuple objects."
  [t1 t2]
  (let [[f1 f2] (map extract-fields [t1 t2])]
    (apply fire-tuple
           (map + f1 f2))))

(defn fire-tseries
  "Creates a `TimeSeries` object from a start period, end period, and
  sequence of timeseries entries. This is appropriate only for
  `FireTuple` entries."
  [start end ts-seq]
  (doto (TimeSeries.)
    (.setStartPeriod start)
    (.setEndPeriod end)
    (.setValues (ArrayList. ts-seq))))

;; ### Fire Predicates

(defn format-datestring
  "Takes a datestring from our fire datasets, formatted as
  `MM/DD/YYYY`, and returns a date formatted as `YYYY-MM-DD`."
  [datestring]
  (let [[month day year] (split datestring #"/")]
    (format "%s-%s-%s" year month day)))

(def
  ^{:doc "Predicate macro to generate a tuple of fire characteristics
  from confidence and temperature."}
  fire-characteristics
  (<- [?conf ?kelvin :> ?tuple]
      (p/full-count ?conf :> ?count)
      (p/filtered-count [330] ?kelvin :> ?temp-330)
      (p/filtered-count [50] ?conf :> ?conf-50)
      (p/bi-filtered-count [330 50] ?conf ?kelvin :> ?both-preds)
      (fire-tuple ?temp-330 ?conf-50 ?both-preds ?count :> ?tuple)))

;; Aggregates a number of firetuples by adding up the values of each
;; `FireTuple` property.

(defaggregateop merge-firetuples
  ([] [0 0 0 0])
  ([state tuple] (map + state (extract-fields tuple)))
  ([state] [(apply fire-tuple state)]))

(defn running-sum
  "Given an accumulator, an initial value and an addition function,
  transforms the input sequence into a new sequence of equal length,
  increasing for each value."
  [acc init add-func tseries]
  (first (reduce (fn [[coll last] new]
                   (let [last (add-func last new)]
                     [(conj coll last) last]))
                 [acc init]
                 tseries)))

;; Special case of `running-sum` for `FireTuple` thrift objects.
(defmapop [running-fire-sum [start end]]
  [tseries]
  (let [empty (fire-tuple 0 0 0 0)]
    (->> tseries
         (running-sum [] empty add-fires)
         (fire-tseries start end))))

;; ## Fire Queries

(defn fire-source
  "Takes a source of textlines, and returns tuples with dataset, date,
  position and value all defined. In this case, the value `?tuple` is
  a `FireTuple` thrift object containing all relevant characteristics
  of fires for that particular day."
  [source]
  (<- [?dataset ?datestring ?t-res ?lat ?lon ?tuple]
      (source ?line)
      (p/mangle ?line :> ?lat ?lon ?kelvin _ _ ?date _ _ ?conf _ _ _)
      (p/add-fields "fire" "01" :> ?dataset ?t-res)
      (format-datestring ?date :> ?datestring)
      (fire-characteristics ?conf ?kelvin :> ?tuple)))

(defn reproject-fires
  "Aggregates fire data at the supplied path by modis pixel at the
  supplied resolution."
  [m-res source]
  (let [fires (fire-source source)]
    (<- [?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple]
        (p/add-fields m-res :> ?m-res)
        (fires ?dataset ?datestring ?t-res ?lat ?lon ?tuple)
        (latlon->modis m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (hv->tilestring ?mod-h ?mod-v :> ?tilestring))))

(defn aggregate-fires
  "Converts the datestring into a time period based on the supplied
  temporal resolution."
  [t-res src]
  (<- [?dataset ?m-res ?new-t-res ?tilestring ?tperiod ?sample ?line ?newtuple]
      (p/add-fields t-res :> ?new-t-res)
      (src ?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple)
      (datetime->period ?new-t-res ?datestring :> ?tperiod)
      (merge-firetuples ?tuple :> ?newtuple)))

(defn fire-series
  "Aggregates fires into timeseries."
  [t-res start end src]
  (let [start (datetime->period t-res start)
        end (datetime->period t-res end)
        length (inc (- end start))
        mk-fire-tseries (p/vals->sparsevec start length (fire-tuple 0 0 0 0))]
    (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?ct-series]
        (src ?dataset ?m-res ?t-res ?tilestring ?tperiod ?sample ?line ?tuple)
        (mk-fire-tseries ?tperiod ?tuple :> _ ?tseries)
        (running-fire-sum [start end] ?tseries :> ?ct-series))))
