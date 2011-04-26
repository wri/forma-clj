(ns forma.source.fire
  (:use cascalog.api
        [clojure.string :only (split)]
        [forma.source.modis :only (latlon->modis
                                   hv->tilestring)])
  (:require [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]))

(def prepath "/Users/sritchie/Desktop/FORMA/FIRE/")
(def testfile (str prepath "MCD14DL.2011074.txt"))

(defn format-datestring
  [datestring]
  (let [[month day year] (split datestring #"/")]
    (format "%s-%s-%s" year month day)))

(defmacro defpredsummer
  [name pred]
  `(defaggregateop ~name
     ([] 0)
     ([count# val#] (if (~pred val#)
                      (inc count#)
                      count#))
     ([count#] [count#])))

(defpredsummer fires-above-330
  (fn [v] (> v 330)))

(defpredsummer conf-above-50
  (fn [v] (> v 50)))

(defaggregateop both-preds
  ([] 0)
  ([count conf temp]
     (if (and (> temp 330)
              (> conf 50))
       (inc count)
       count))
  ([count] [count]))

(def fire-characteristics
  (<- [?conf ?kelvin :> ?temp-330 ?conf-50 ?both-preds ?max-t]
      ((c/juxt #'fires-above-330 #'c/max) ?kelvin :> ?temp-330 ?max-t)
      (conf-above-50 ?conf :> ?conf-50)
      (both-preds ?conf ?kelvin :> ?both-preds)))

(defn fire-source
  "Takes a source of textlines, and returns 2-tuples with latitude and
  longitude."
  [source t-res]
  (<- [?dataset ?datestring ?t-res ?lat ?lon ?temp-330 ?conf-50 ?both-preds ?max-t]
      (source ?line)
      (identity "fire" :> ?dataset)
      (identity "01" :> ?t-res)
      (format-datestring ?date :> ?datestring)
      (p/mangle ?line :> ?lat ?lon ?kelvin _ _ ?date _ _ ?conf _ _ _)
      (fire-characteristics ?conf ?kelvin :> ?temp-330 ?conf-50 ?both-preds ?max-t)))

(defn rip-fires
  "Aggregates fire data at the supplied path by modis pixel at the
  supplied resolution."
  [m-res t-res path]
  (let [fires (fire-source (hfs-textline path) t-res)]
    (<- [?dataset ?m-res ?t-res ?tilestring
         ?datestring ?sample ?line ?fire-count ?temp-330 ?conf-50 ?both-preds ?max-t]
        (fires ?dataset ?datestring ?t-res ?lat ?lon ?temp-330 ?conf-50 ?both-preds ?max-t)
        (latlon->modis m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (hv->tilestring ?mod-h ?mod-v :> ?tilestring)
        (identity m-res :> ?m-res)
        (c/count ?fire-count))))

(defn run-rip
  "Rips apart fires!"
  [count]
  (?- (stdout)
      (c/first-n
       (rip-fires "1000" "32" testfile)
       count)))
