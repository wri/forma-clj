(ns forma.source.fire
  (:use cascalog.api
        [forma.date-time :only (datetime->period current-period)]
        [clojure.string :only (split)]
        [forma.matrix.utils :only (sparse-expander)]
        [forma.source.modis :only (latlon->modis
                                   hv->tilestring)])
  (:require [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]
            [cascalog.vars :as v])
  (:import [forma.schema FireTuple TimeSeries]
           [java.util ArrayList]))

;; ### Fire Predicates

(defn format-datestring
  [datestring]
  (let [[month day year] (split datestring #"/")]
    (format "%s-%s-%s" year month day)))

(defmacro defpredsummer
  "Generates cascalog defaggregateops for counting items that satisfy
  some custom predicate. Defaggregateops don't allow anonymous
  functions, so we went this route instead."
  [name vals pred]
  `(defaggregateop ~name
     ([] 0)
     ([count# ~@vals] (if (~pred ~@vals)
                      (inc count#)
                      count#))
     ([count#] [count#])))

(defpredsummer fires-above-330
  [val] #(> % 330))

(defpredsummer conf-above-50
  [val] #(> % 50))

(defpredsummer per-day
  [val] identity)

(defpredsummer both-preds
  [conf temp]
  (fn [c t] (and (> t 330)
                (> c 50))))

(defn tupleize
  [t-above-330 c-above-50 both-preds count]
  (FireTuple. t-above-330 c-above-50 both-preds count))

(def
  ^{:doc "Generates a tuple of fire characteristics from confidence
  and temperature."}
  fire-characteristics
  (<- [?conf ?kelvin :> ?tuple]
      ((c/juxt #'conf-above-50 #'per-day) ?conf :> ?conf-50 ?count)
      (fires-above-330 ?kelvin :> ?temp-330)
      (both-preds ?conf ?kelvin :> ?both-preds)
      (tupleize ?temp-330 ?conf-50 ?both-preds ?count :> ?tuple)))

;; ## Fire Queries

(defn mk-ts
  "TODO: DOCS"
  [start end ts-seq]
  (doto (TimeSeries.)
    (.setStartPeriod start)
    (.setEndPeriod end)
    (.setValues (ArrayList. ts-seq))))

(defbufferop [sparse-expansion [t-res start-date end-date missing-val]]
  {:doc "Receives 2-tuple pairs of the form `<idx, val>`, and inserts
  each `val` into a sparse vector of the supplied length at the
  corresponding `idx`. `missing-val` will be substituted for any
  missing value."}
  [tuples]
  (let [one (datetime->period t-res start-date)
        two (datetime->period t-res end-date)
        length (inc (- two one))
        ts (mk-ts one
                  two
                  (sparse-expander missing-val tuples
                                   :start one
                                   :length length))]
    
    [[ts]]))

(defn fire-source
  "Takes a source of textlines, and returns 2-tuples with latitude and
  longitude."
  [source]
  (let [vs (v/gen-non-nullable-vars 5)]
    (<- [?dataset ?datestring ?t-res ?lat ?lon ?tuple]
        (source ?line)
        (identity "fire" :> ?dataset)
        (identity "01" :> ?t-res)
        (format-datestring ?date :> ?datestring)
        (p/mangle ?line :> ?lat ?lon ?kelvin _ _ ?date _ _ ?conf _ _ _)
        (fire-characteristics ?conf ?kelvin :> ?tuple))))

(defn rip-fires
  "Aggregates fire data at the supplied path by modis pixel at the
  supplied resolution."
  [m-res source]
  (let [fires (fire-source source)]
    (<- [?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple]
        (fires ?dataset ?datestring ?t-res ?lat ?lon ?tuple)
        (latlon->modis m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (hv->tilestring ?mod-h ?mod-v :> ?tilestring)
        (identity m-res :> ?m-res))))

(defn add-fires
  "Adds together two FireTuple objects."
  [t1 t2]
  (tupleize (+ (.temp330 t1) (.temp330 t2))
            (+ (.conf50 t1) (.conf50 t2))
            (+ (.bothPreds t1) (.bothPreds t2))
            (+ (.count t1) (.count t2))))


;; Combines various fire tuples into one.
(defaggregateop combine
  ([] (FireTuple. 0 0 0 0))
  ([state tuple] (add-fires state tuple))
  ([state] [state]))

(defn aggregate-fires
  "Converts the datestring into a time period based on the supplied
  temporal resolution."
  [t-res src]
  (<- [?dataset ?m-res ?new-t-res ?tilestring ?tperiod ?sample ?line ?newtuple]
      (datetime->period ?new-t-res ?datestring :> ?tperiod)
      (identity t-res :> ?new-t-res)
      (combine ?tuple :> ?newtuple)
      (src ?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple)))

(defn fire-series
  "Aggregates fires into timeseries."
  [t-res start end src]
  (let [empty (FireTuple. 0 0 0 0)]
    (<- [?dataset ?m-res ?t-res ?tilestring ?tperiod ?sample ?line ?tseries]
        (src ?dataset ?m-res ?t-res ?tilestring ?tperiod ?sample ?line ?tuple)
        (:sort ?tperiod)
        (sparse-expansion [t-res start end empty] ?tperiod ?tuple :> ?tseries))))

(defn run-rip
  "Rips apart fires!"
  [t-res start end]
  (?- (stdout)
      (->> (rip-fires "1000" new-fire-tap)
           (aggregate-fires t-res)
           (fire-series t-res start end))))
