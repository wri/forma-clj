(ns forma.source.fire
  (:use cascalog.api
        [forma.date-time :only (datetime->period current-period)]
        [clojure.string :only (split)]
        [forma.matrix.utils :only (sparse-expander matrix-of)]
        [forma.source.modis :only (latlon->modis
                                   hv->tilestring)])
  (:require [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]
            [cascalog.vars :as v])
  (:import [forma.schema FireTuple TimeSeries]))

(def prepath "/Users/sritchie/Desktop/FORMA/FIRE/")
(def testfile (str prepath "MCD14DL.2011074.txt"))

(defn format-datestring
  [datestring]
  (let [[month day year] (split datestring #"/")]
    (format "%s-%s-%s" year month day)))

;; ### Fire Predicates

(defmacro defpredsummer
  [name vals pred]
  `(defaggregateop ~name
     ([] 0)
     ([count# ~@vals] (if (~pred ~@vals)
                      (inc count#)
                      count#))
     ([count#] [count#])))

(defpredsummer fires-above-330
  [val]
  (fn [v] (> v 330)))

(defpredsummer conf-above-50
  [val]
  (fn [v] (> v 50)))

(defpredsummer per-day
  [val]
  identity)

(defpredsummer both-preds
  [conf temp]
  (fn [c t] (and (> t 330)
                (> c 50))))

(def fire-characteristics
  (<- [?conf ?kelvin :> ?temp-330 ?conf-50 ?both-preds ?max-t ?count]
      ((c/juxt #'fires-above-330 #'c/max) ?kelvin :> ?temp-330 ?max-t)
      (conf-above-50 ?conf :> ?conf-50)
      (per-day ?conf :> ?count)
      (both-preds ?conf ?kelvin :> ?both-preds)))

;; ## Fire Queries

(defn tupleize
  [t-above-330 c-above-50 both-preds max-t count]
  (FireTuple. max-t t-above-330 c-above-50 both-preds count))

(defn fire-source
  "Takes a source of textlines, and returns 2-tuples with latitude and
  longitude."
  [source t-res]
  (let [vs (v/gen-non-nullable-vars 5)]
    (<- [?dataset ?datestring ?t-res ?lat ?lon ?tuple]
        (source ?line)
        (identity "fire" :> ?dataset)
        (identity "01" :> ?t-res)
        (format-datestring ?date :> ?datestring)
        (p/mangle ?line :> ?lat ?lon ?kelvin _ _ ?date _ _ ?conf _ _ _)
        (fire-characteristics ?conf ?kelvin :>> vs)
        (tupleize :<< vs :> ?tuple))))

(defn rip-fires
  "Aggregates fire data at the supplied path by modis pixel at the
  supplied resolution."
  [m-res t-res source]
  (let [fires (fire-source source t-res)]
    (<- [?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple]
        (fires ?dataset ?datestring ?t-res ?lat ?lon ?tuple)
        (latlon->modis m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (hv->tilestring ?mod-h ?mod-v :> ?tilestring)
        (identity m-res :> ?m-res))))

(def new-fire-tap
  (memory-source-tap
   [["-4.214,152.190,319.9,1.6,1.2,01/15/2011,0035,T,0,5.0,301.3,27.8"]
   ["-26.464,148.237,312.2,1.7,1.3,01/15/2011,0040,T,30,5.0,288.3,16.9"]
   ["-28.314,150.342,329.3,2.5,1.5,01/15/2011,0040,T,82,5.0,301.9,83.4"]
   ["-27.766,140.252,317.3,1.1,1.0,01/15/2011,0040,T,63,5.0,305.8,15.4"]
   ["-29.059,150.453,331.3,2.6,1.6,01/15/2011,0040,T,84,5.0,302.4,91.2"]
   ["-29.059,150.453,331.3,2.6,1.6,01/15/2011,0040,T,84,5.0,302.4,91.2"]
   ["-29.059,150.453,331.3,2.6,1.6,02/15/2011,0040,T,84,5.0,302.4,91.2"]
   ["-29.059,150.453,331.3,2.6,1.6,03/15/2011,0040,T,84,5.0,302.4,91.2"]
   ["-29.063,150.447,327.3,2.6,1.6,04/15/2011,0040,T,80,5.0,301.9,67.0"]
   ["-28.963,148.843,322.2,2.0,1.4,01/15/2011,0040,T,75,5.0,302.9,26.5"]
   ["-28.971,148.801,329.4,2.0,1.4,01/15/2011,0040,T,82,5.0,303.2,51.0"]
   ["-28.975,148.842,328.7,2.0,1.4,01/15/2011,0040,T,82,5.0,303.2,50.5"]
   ["-29.262,150.233,328.2,2.5,1.5,01/15/2011,0040,T,81,5.0,301.1,64.9"]]))

(defbufferop [sparse-expansion [missing-val]]
  {:doc "Receives 2-tuple pairs of the form `<idx, val>`, and inserts
  each `val` into a sparse vector of the supplied length at the
  corresponding `idx`. `missing-val` will be substituted for any
  missing value."}
  [tuples]
  (let [one (ffirst tuples)
        two (current-period "32")
        length (inc (- two one))
        ts (doto (TimeSeries.)
             (.setStartPeriod one)
             (.setEndPeriod two))]
    (doseq [tuple (sparse-expander missing-val tuples :length length)]
      (.addToValues ts tuple))
    [[ts]]))

(defn fire-series
  [src]
  (<- [?dataset ?m-res ?new-t-res ?tilestring ?sample ?line ?tseries]
      (datetime->period ?new-t-res ?datestring :> ?tperiod)
      (src ?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple)
      (identity "32" :> ?new-t-res)
      (:sort ?tperiod)
      (sparse-expansion [(FireTuple. 0 0 0 0 0)] ?tperiod ?tuple :> ?tseries)))

(defn run-rip
  "Rips apart fires!"
  [count]
  (?- (stdout)
      (fire-series (rip-fires "1000" "32" new-fire-tap))))
