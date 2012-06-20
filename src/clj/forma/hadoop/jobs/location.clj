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

(defn global-dims
  "accepts a spatial resolution and returns a tuple of the form [row
  column] with the total number of rows and columns of the entire
  world at the supplied resolution."
  [sres]
  (let [num-pixels (r/pixels-at-res sres)]
    (map (partial * num-pixels) [r/v-tiles r/h-tiles])))

(defrecord TileRowCol [mod-h mod-v sample line])
(defrecord GlobalRowCol [rowcol])
(defrecord GlobalIndex [idx])
(defrecord WindowRowCol [rowcol])
(defrecord WindowIndex [idx])

(defn TileRowCol*
  ([coll]
     (let [[h v s l] coll]
       (TileRowCol. h v s l)))
  ([h v s l] (TileRowCol. h v s l)))

(defmulti global-index (fn [sres t] (class t)))

(defmethod global-index TileRowCol [sres t]
  (let [global-row (+ (* (:mod-v t) (r/pixels-at-res sres)) (:line t))
        global-col (+ (* (:mod-h t) (r/pixels-at-res sres)) (:sample t))
        [nrows ncols] (global-dims sres)]
    (util/rowcol->idx nrows ncols global-row global-col)))

(defmethod global-index GlobalRowCol [sres t]
  (let [[row col] (:rowcol t)
        [nrows ncols] (global-dims sres)]
    (util/rowcol->idx nrows ncols row col)))

(defmulti global-rowcol (fn [sres t] (class t)))

(defmethod global-rowcol TileRowCol [sres t]
  (let [num-pixels (r/pixels-at-res sres)]
    [(+ (:line t) (* num-pixels (:mod-v t)))
     (+ (:sample t) (* num-pixels (:mod-h t)))]))

(defmethod global-rowcol GlobalIndex [sres t]
  (let [[nrows ncols] (global-dims sres)]
    (util/idx->rowcol nrows ncols (:idx t))))

(defmulti window-rowcol (fn [sres window-map t] (class t)))

(defmethod window-rowcol WindowIndex [sres window-map t]
  (util/idx->rowcol (:height window-map) (:width window-map) (:idx t)))

(defmethod window-rowcol GlobalRowCol [sres window-map t]
  {:post [(< (first %) (+ (window-map :height) (first (window-map :topleft))))
          (< (second %) (+ (window-map :height) (second (window-map :topleft))))
          (every? pos? %)]}
  (let [topleft (window-map :topleft)]
    (vec (map - (:rowcol t) topleft))))

(defmethod window-rowcol GlobalIndex [sres window-map t]
  (window-rowcol sres window-map (global-rowcol sres t)))

(defmulti window-idx (fn [sres window-map t] (class t)))

(defmethod window-idx WindowRowCol [sres window-map t]
  (let [[row col] (:rowcol t)]
    (util/rowcol->idx (:height window-map) (:width window-map) row col)))

(defmethod window-idx GlobalIndex [sres window-map t]
  (window-idx (window-rowcol sres window-map t)))

(defmethod window-idx GlobalRowCol [sres window-map t]
  (window-idx (window-rowcol sres window-map t)))
