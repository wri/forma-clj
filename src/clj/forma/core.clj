;; This namespace is a hub for all others, and not really complete in
;; its own right. We define various forma constants, and hold some of
;; the test queries in here for now. Some of these bad boys need to
;; get moved over to tests.

(ns forma.core
  (:use cascalog.api
        (forma [hadoop :only (all-files)]))
  (:require (cascalog [vars :as v]
                      [ops :as c])
            (forma [hdf :as h]
                   [rain :as r])))

;; ### FORMA Constants
;;
;; Aside from our EC2 information, this is the only data we need to
;; supply for the first phase of operations. Everything else should be
;; able to remain local to individual modules.

(def forma-subsets #{:ndvi :evi :qual :reli})

;; Arbitrary number of pixels slurped at a time off of a MODIS raster
;; band. For 1km data, each MODIS tile is 1200x1200 pixels; dealing
;; with each pixel individually would incur unacceptable IO costs
;; within hadoop. We currently fix the chunk size at 24,000, resulting
;; in 60 chunks per 1km data. Sharper resolution -> more chunks!

(def chunk-size 24000)

;; ### Demonstration Queries

(defn chunk-test
  "Cascalog query that takes a directory containing MODIS HDF files, or
  a link directly to such a file, totals up the # of chunks per file,
  and displays the count alongside some other nice metadata. This
  method is a proof of concept for the hadoop chunking system; The
  fact that it works shows that chunking is occurring, and the
  individual chunks are being serialized over multiple hadoop jobs."
  [dir]
  (let [source (all-files dir)
        chunks (h/modis-chunks source forma-subsets chunk-size)]
    (?<- (stdout)
         [?dataset ?res ?tile-h ?tile-v ?period ?count]
         (chunks ?dataset ?res ?tile-h ?tile-v ?period ?chunkid ?chunk)
         (c/count ?count))))

(defn rain-test
  "Like chunk-test, but works for NOAA PRECL data files."
  [m-res ll-res c-size tile-seq dir]
  (let [source (all-files dir)
        chunks (r/rain-chunks m-res ll-res c-size tile-seq source)]
    (?<- (stdout) [?dataset ?res ?period]
         (chunks ?dataset ?res ?mod-h ?mod-v ?period ?chunkid ?chunk)
         (c/count ?count))))