(ns forma.hoptree
  (:require [forma.matrix.utils :as util]
            [forma.reproject :as r]
            [forma.hadoop.predicate :as p]))

;; An API to access the location of MODIS pixels at three levels: (1)
;; Within the tile, which is identified by a tile location and the
;; pixel location within the tile.  The tile identifier follows the
;; standard form: mod-h, mod-v, sample, line. (2) Global position, as
;; if MODIS tiles did not exist and the world was a single MODIS grid
;; at the supplied resolution. (3) An arbitrary, rectangular window,
;; which is defined by the pixel at the top-left corner of the window,
;; the width in pixels, and the height in pixels.

;; For levels (2) and (3), we are interested in both the [row column]
;; of the pixel as well as the unique index that is found by counting
;; from the top left and travelling left-to-right and then
;; top-to-bottom.  The unique index is not interesting at the tile
;; level, or at least we have never needed that index.

;; Everything is zero-indexed, so that the top-left corner of any
;; window will have row and column defined by [0 0] and index 0.

;; Note that if the global index of the pixel under consideration
;; falls outside of the specified window, then the conversion will
;; throw an error.

;; TODO: remove redundancy from multimethods

(defn global-dims
  "accepts a spatial resolution and returns a tuple of the form [row
  column] with the total number of rows and columns of the entire
  world at the supplied resolution.

  EXAMPLE:
  (global-dims \"500\") => (43200 86400)"
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

(defn GlobalRowCol*
  ([coll] (GlobalRowCol. coll))
  ([r c]  (GlobalRowCol. [r c])))

(defn WindowRowCol*
  ([coll] (WindowRowCol. coll))
  ([r c]  (WindowRowCol. [r c])))

(defn WindowIndex*
  ([idx] (WindowIndex. idx)))

(defn GlobalIndex*
  ([idx] (GlobalIndex. idx)))

(defmulti global-index (fn [sres t & [window-map]] (class t)))

(defmethod global-index TileRowCol [sres t]
  (let [global-row (+ (* (:mod-v t) (r/pixels-at-res sres)) (:line t))
        global-col (+ (* (:mod-h t) (r/pixels-at-res sres)) (:sample t))
        [nrows ncols] (global-dims sres)]
    (util/rowcol->idx nrows ncols global-row global-col)))

(defmethod global-index GlobalRowCol [sres t]
  (let [[row col] (:rowcol t)
        [nrows ncols] (global-dims sres)]
    (util/rowcol->idx nrows ncols row col)))

(defmethod global-index WindowRowCol [sres t & [window-map]]
  (let [rowcol (map + (window-map :topleft-rowcol) (:rowcol t))]
    (global-index sres (GlobalRowCol. rowcol))))

(defmulti window-rowcol (fn [sres window-map t] (class t)))

(defmethod window-rowcol WindowIndex [sres window-map t]
  (util/idx->rowcol (:height window-map) (:width window-map) (:idx t)))

(defmethod global-index WindowIndex [sres t & [window-map]]
  (global-index sres (window-rowcol sres window-map t) window-map))

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

(defn in-window?
  "returns true if the tuple of the form [row col] is within the
  window that is defined by window-map that has
  keys :topleft-rowcol, :height, and :width."
  [window-map tuple]
  (and (< (inc (first tuple)) (+ (window-map :height)
                                 (first (window-map :topleft-rowcol))))
       (< (inc (second tuple)) (+ (window-map :width)
                                  (second (window-map :topleft-rowcol))))
       (every? pos? (map inc tuple))))

(defmethod window-rowcol GlobalRowCol [sres window-map t]
  {:pre [(= (class (:topleft-rowcol window-map)) clojure.lang.PersistentVector)]
   :post [(in-window? window-map %)]}
  (let [topleft (window-map :topleft-rowcol)]
    (vec (map - (:rowcol t) topleft))))

(defmethod window-rowcol GlobalIndex [sres window-map t]
  (window-rowcol sres window-map (GlobalRowCol. (global-rowcol sres t))))

(defmulti window-idx (fn [sres window-map t] (class t)))

(defmethod window-idx WindowRowCol [sres window-map t]
  (let [[row col] (:rowcol t)]
    (util/rowcol->idx (:height window-map) (:width window-map) row col)))

(defmethod window-idx GlobalIndex [sres window-map t]
  (window-idx sres window-map (WindowRowCol. (window-rowcol sres window-map t))))

(defmethod window-idx GlobalRowCol [sres window-map t]
  (window-idx sres window-map (WindowRowCol. (window-rowcol sres window-map t))))

(defmulti neighbor-idx (fn [sres t] (class t)))

(defmethod neighbor-idx GlobalIndex [sres t]
  (let [idx (:idx t)
        total-row (* (r/pixels-at-res sres) r/h-tiles)
        short-row (fn [x] [(dec x) x (inc x)])]
    (flatten
     (map short-row [(- idx total-row) idx (+ idx total-row)]))))
