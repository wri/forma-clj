(ns forma.source.fire
  (:use cascalog.api
        [clojure.string :only (split)]
        [forma.source.modis :only (latlon->modis)])
  (:require [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]))

(defmapop [format-datestring [t-res]]
  [datestring]
  (let [[month day year] (split datestring #"/")]
    (format "%s-%s-%s" year month day)))

(defn fire-source
  "Takes a source of textlines, and returns 2-tuples with latitude and
  longitude."
  [source t-res]
  (<- [?dataset ?lat ?lon ?datestring ?kelvin]
      (source ?line)
      (identity "fire" :> ?dataset)
      (format-datestring [] ?date :> ?datestring)
      (p/mangle ?line :> ?lat ?lon ?kelvin _ _ ?date _ _ _ _ _ _)))

(defn rip-fires
  "Aggregates fire data at the supplied path by modis pixel at the
  supplied resolution."
  [m-res t-res path]
  (let [fires (fire-source (hfs-textline path) t-res)]
    (<- [?dataset ?mod-h ?mod-v ?sample ?line ?datestring ?fire-count ?max-t]
        (fires ?dataset ?lat ?lon ?datestring ?kelvin)
        (> ?kelvin 330)
        (latlon->modis m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (c/max ?kelvin :> ?max-t)
        (c/count ?fire-count))))

(def prepath "/Users/sritchie/Desktop/FORMA/FIRE/")
(def testfile (str prepath "MCD14DL.2011074.txt"))

(defn run-rip
  "Rips apart fires!"
  [count]
  (?- (stdout)
      (c/first-n (rip-fires "1000" "32" testfile)
                 count)))
