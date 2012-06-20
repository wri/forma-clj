(ns forma.hadoop.jobs.location
  (:use cascalog.api
        [forma.reproject :only (modis->latlon)])
  (:require [cascalog.ops :as c]
            [forma.matrix.utils :as util]
            [forma.reproject :as r]
            [forma.thrift :as thrift]
            [incanter.charts :as chart]
            [incanter.stats :as stat]
            [incanter.core :as i]
            [forma.hadoop.jobs.local :as local]
            [forma.hadoop.predicate :as p]))


(defrecord TilePosition [mod-h mod-v sample line])

(defprotocol GlobalPosition
  (global-row [t sres])
  (global-col [t sres]))

(defprotocol GlobalIndex
  (global-idx [t sres]))

(extend-type TilePosition
  GlobalPosition
  (global-row [t sres] (+ (* (:mod-v t) (r/pixels-at-res sres)) (:line t)))
  (global-col [t sres] (+ (* (:mod-h t) (r/pixels-at-res sres)) (:sample t))))

(extend-type TilePosition
  GlobalIndex
  (global-idx [t sres]
    (let [[nrows ncols] (local/global-dims sres)]
      (util/rowcol->idx
       nrows ncols                  
       (global-row t sres)
       (global-col t sres)))))

(defprotocol WindowPosition
  (window-row [t sres topmost-row])
  (window-col [t sres leftmost-col]))

(defprotocol WindowIndex
  (window-idx [t sres topmost-row leftmost-col height width]))

(extend-type TilePosition
  WindowPosition
  (window-row [t sres topmost-row] (- (global-row t sres) topmost-row))
  (window-col [t sres leftmost-col] (- (global-col t sres) leftmost-col)))

(extend-type TilePosition
  WindowIndex
  (window-idx [t sres topmost-row leftmost-col height width]
    (util/rowcol->idx
     height
     width
     (window-row t sres topmost-row)
     (window-col t sres leftmost-col))))
