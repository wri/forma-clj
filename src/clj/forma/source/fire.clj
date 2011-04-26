(ns forma.source.fire
  (:use cascalog.api
        [forma.date-time :only (periodize)]
        [forma.matrix.utils :only (idx->colrow sparse-vector)]
        [forma.reproject :only (wgs84-index
                                dimensions-at-res)]
        [forma.source.modis :only (latlon->modis
                                   pixels-at-res)]
        [clojure.string :only (split)])
  (:require [cascalog.ops :as c]
            [forma.hadoop.predicate :as p]))

(def fire-tap
  (memory-source-tap
   [["455888,30,A,-12.687,21.323,312.8,287.1,198,47.5,80"]
    ["455889,30,A,-14.141,25.583,319.8,288,571,24.7,90"]
    ["455890,30,A,-14.142,25.592,318.1,288.7,572,22.6,77"]
    ["455891,30,A,-14.159,25.58,308.9,288.8,571,12.6,19"]
    ["455892,30,A,-14.161,25.589,308.6,288.9,572,12.3,47"]
    ["455893,30,A,-14.171,25.597,331.4,289.8,573,39.7,100"]
    ["455894,30,A,-14.172,25.607,325.8,289.1,574,32.2,100"]
    ["455895,30,A,-15.288,29.643,313.5,289.8,995,26.3,82"]
    ["455896,30,A,-15.872,30.327,305.7,289.8,1057,18.1,36"]
    ["455897,30,A,-25.555,29.305,311,271.8,1117,47.8,73"]]))

(defn mangle
  "Rips apart lines in a fires dataset."
  [line]
  (map (fn [val]
         (try (Float. val)
              (catch Exception _ val)))
       (split line #",")))

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
      (mangle ?line :> ?lat ?lon ?kelvin _ _ ?date _ _ _ _ _ _)))

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

(defn to-vector [x y]
  [[x y]])

(defn sample-aggregator
  "Takes a samples and line generator, and stitches lines back
  together. "
  [point-source]
  (let [sample-agger (p/vals->sparsevec 1200 [0 0])]
    (<- [?dataset ?mod-h ?mod-v ?tperiod ?line ?line-vec-col ?line-vec]
        (point-source ?dataset ?mod-h ?mod-v ?sample ?line ?tperiod ?count ?max-t)
        (to-vector ?count ?max-t :> ?values)
        (sample-agger ?sample ?values :> ?line-vec-col ?line-vec))))

;; Stitches lines together into a window!
;; TODO: Docs
(defn line-aggregator
  "Stitches lines back together into little windows."
  [point-source]
  (let [line-source (sample-aggregator point-source)
        line-agger (p/vals->sparsevec 1200 (-> 1200
                                               (repeat [0 0])
                                               vec))]
    (<- [?dataset ?t-period ?tile-h ?tile-v ?chunkid ?chunk]
        (line-source ?dataset ?tile-h ?tile-v ?t-period ?line ?window-col ?line-vec)
        (line-agger ?line ?line-vec :> ?chunkid ?chunk))))

(def prepath "/Users/sritchie/Desktop/FORMA/FIRE/")

(defn run-rip
  "Rips apart fires!"
  [count]
  (?- (stdout)
      (c/first-n (line-aggregator (rip-fires "1000"
                                             "32"
                                             (str prepath "MCD14DL.2011074.txt")))
                 count)))
