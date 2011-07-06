(ns forma.hadoop.jobs.process-rain
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set)]
        [forma.utils :only (weighted-mean)])
  (:require [cascalog.ops :as c]
            [forma.date-time :as date]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.source.modis :as m]
            [forma.source.static :as static])
  (:gen-class))

(def gadm-tap  (hfs-seqfile "s3n://redddata/gadm/1000-00/*/*/"))
(def precl-tap  (hfs-seqfile "s3n://redddata/precl/1000-32/*/*/"))

(defn rain-tap
  "TODO: Very similar to extract-tseries. Consolidate."
  [rain-src]
  (<- [?mod-h ?mod-v ?sample ?line ?date ?val]
      (rain-src _ ?s-res _ ?tilestring ?date ?chunkid ?chunk)
      (io/count-vals ?chunk :> ?chunk-size)
      (p/struct-index 0 ?chunk :> ?pix-idx ?val)
      (m/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
      (m/tile-position ?s-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defbufferop weighted-avg [tuples]
  [(apply weighted-mean (flatten tuples))])

(defn run-rain
  [gadm-src rain-src]
  (let [gadm-src (static/static-tap gadm-src)
        rain-src (rain-tap rain-src)
        join (<- [?gadm ?mod-h ?mod-v ?sample ?line ?date ?count ?avg-rain]
                 (gadm-src _ ?mod-h ?mod-v ?sample ?line ?gadm)
                 (rain-src ?mod-h ?mod-v ?sample ?line ?date ?rain)
                 (c/count ?count)
                 (c/avg ?rain :> ?avg-rain))]
    (<- [?gadm ?date ?weighted-avg]
        (join ?gadm ?mod-h ?mod-v ?sample ?line ?date ?count ?avg-rain)
        (weighted-avg ?avg-rain ?count :> ?weighted-avg))))

(defn -main [path]
  (?- (io/myhfs-textline path)
      (run-rain gadm-tap precl-tap)))
