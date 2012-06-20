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

(defn global-dims [s-res]
  (let [num-pixels (r/pixels-at-res s-res)]
    (map (partial * num-pixels) [r/v-tiles r/h-tiles])))

(defrecord TileRowCol [mod-h mod-v sample line sres])
(defrecord GlobalRowCol [row col sres])
(defrecord GlobalIndex [idx sres])
(defrecord WindowRowCol [row col topleft-rowcol sres])
(defrecord WindowIndex [idx topleft-idx sres])

(defmulti global-index class)

(defmethod global-index TileRowCol [t]
  (let [sres (:sres t)
        global-row (+ (* (:mod-v t) (r/pixels-at-res sres)) (:line t))
        global-col (+ (* (:mod-h t) (r/pixels-at-res sres)) (:sample t))
        [nrows ncols] (global-dims sres)]
    (util/rowcol->idx nrows ncols global-row global-col)))

(defmethod global-index GlobalRowCol [t]
  (let [[nrows ncols] (global-dims (:sres t))]
    (util/rowcol->idx nrows ncols (:row t) (:col t))))

(defmulti global-rowcol class)

(defmethod global-rowcol TileRowCol [t]
  (let [sres (:sres t)
        num-pixels (r/pixels-at-res sres)]
    [(+ (:line t) (* num-pixels (:mod-v t)))
     (+ (:sample t) (* num-pixels (:mod-h t)))]))

(defmethod global-rowcol GlobalIndex [t]
  (let [[nrows ncols] (global-dims (:sres t))]
    (util/idx->rowcol nrows ncols (:idx t))))



;; (defprotocol get-global-position
;;   (global-row [t sres])
;;   (global-col [t sres]))

;; (defprotocol get-global-index
;;   (global-idx [t sres]))

;; (extend-type TilePosition
;;   get-global-position)

;; (extend-type TilePosition
;;   get-global-index
;;   (global-idx [t sres]
;;     (let [[nrows ncols] (local/global-dims sres)]
;;       (util/rowcol->idx
;;        nrows ncols                  
;;        (global-row t sres)
;;        (global-col t sres)))))

;; (defprotocol WindowPosition
;;   (window-row [t sres topmost-row])
;;   (window-col [t sres leftmost-col]))

;; (defprotocol WindowIndex
;;   (window-idx [t sres topmost-row leftmost-col height width]))

;; (extend-type TilePosition
;;   WindowPosition
;;   (window-row [t sres topmost-row] (- (global-row t sres) topmost-row))
;;   (window-col [t sres leftmost-col] (- (global-col t sres) leftmost-col)))

;; (extend-type TilePosition
;;   WindowIndex
;;   (window-idx [t sres topmost-row leftmost-col height width]
;;     (util/rowcol->idx
;;      height
;;      width
;;      (window-row t sres topmost-row)
;;      (window-col t sres leftmost-col))))


