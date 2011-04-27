(ns forma.source.fire
  (:use cascalog.api
        [clojure.string :only (split)]
        [forma.source.modis :only (latlon->modis
                                   hv->tilestring)])
  (:require [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]
            [cascalog.vars :as v])
  (:import [forma.schema FireTuple]))

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
  [max-t t-above-330 c-above-50 both-preds count]
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
  [m-res t-res path]
  (let [fires (fire-source (hfs-textline path) t-res)]
    (<- [?dataset ?m-res ?t-res ?tilestring ?datestring ?sample ?line ?tuple]
        (fires ?dataset ?datestring ?t-res ?lat ?lon ?tuple)
        (latlon->modis m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (hv->tilestring ?mod-h ?mod-v :> ?tilestring)
        (identity m-res :> ?m-res))))

(defn run-rip
  "Rips apart fires!"
  [count]
  (?- (stdout)
      (c/first-n
       (rip-fires "1000" "32" testfile)
       count)))
