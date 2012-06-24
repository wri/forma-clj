(ns forma.hadoop.jobs.neighbors
  (:use cascalog.api
        forma.hoptree)
  (:require [cascalog.ops :as c]
            [forma.matrix.utils :as util]
            [forma.matrix.walk :as walk]
            [incanter.core :as i]))

(def sm-src [["a" 0 0 20 20 1]
             ["a" 0 0 21 20 1]
             ["a" 0 0 20 21 1]
             ["a" 0 0 21 21 1]
             ["b" 0 0 20 20 1]
             ["b" 0 0 21 20 1]
             ["b" 0 0 20 21 1]
             ["b" 0 0 21 21 1]])

(def lg-src [["a" 0 0 19 19 1]
             ["a" 0 0 19 20 1]
             ["a" 0 0 19 21 1]
             ["a" 0 0 19 22 1]
             ["a" 0 0 20 19 1]
             ["a" 0 0 20 20 1]
             ["a" 0 0 20 21 1]
             ["a" 0 0 20 22 1]
             ["a" 0 0 21 19 1]
             ["a" 0 0 21 20 1]
             ["a" 0 0 21 21 1]
             ["a" 0 0 21 22 1]
             ["a" 0 0 22 19 1]
             ["a" 0 0 22 20 1]
             ["a" 0 0 22 21 1]
             ["a" 0 0 22 22 1]
             ["a" 0 0 23 19 1]
             ["a" 0 0 23 20 1]
             ["a" 0 0 23 21 1]
             ["a" 0 0 23 22 1]
             ["a" 0 0 24 19 1]
             ["a" 0 0 24 20 1]
             ["a" 0 0 24 21 1]
             ["a" 0 0 24 22 1]
             ["b" 0 0 19 19 1]
             ["b" 0 0 19 20 1]
             ["b" 0 0 19 21 1]
             ["b" 0 0 19 22 1]
             ["b" 0 0 20 19 1]
             ["b" 0 0 20 20 1]
             ["b" 0 0 20 21 1]
             ["b" 0 0 20 22 1]
             ["b" 0 0 21 19 1]
             ["b" 0 0 21 20 1]
             ["b" 0 0 21 21 1]
             ["b" 0 0 21 22 1]
             ["b" 0 0 22 19 1]
             ["b" 0 0 22 20 1]
             ["b" 0 0 22 21 1]
             ["b" 0 0 22 22 1]
             ["b" 0 0 23 19 1]
             ["b" 0 0 23 20 1]
             ["b" 0 0 23 21 1]
             ["b" 0 0 23 22 1]
             ["b" 0 0 24 19 1]
             ["b" 0 0 24 20 1]
             ["b" 0 0 24 21 1]
             ["b" 0 0 24 22 1]])

(defmapcatop neighbors
  [idx & {:keys [sres] :or {sres "500"}}]
  (neighbor-idx sres (GlobalIndex* idx)))

(defn neighbor-src [pixel-modis-src sres]
  (<- [?neigh-id]
      (pixel-modis-src ?gadm ?h ?v ?s ?l ?val)
      (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
      (global-index sres ?tile-pixel :> ?idx)
      (neighbors ?idx :> ?neigh-id)
      (:distinct true)))

(defn window-attribute-src [pixel-modis-src full-modis-src sres]
  (let [n-src (neighbor-src pixel-modis-src sres)]
    (<- [?gadm ?idx ?val]
        (n-src ?idx)
        (full-modis-src ?gadm ?h ?v ?s ?l ?val)
        (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
        (global-index sres ?tile-pixel :> ?idx)
        (:distinct true))))

(defn window-map
  "note height and width take into account the zero index."
  [sres global-idx-coll]
  (let [rowcol (fn [x] (global-rowcol sres (GlobalIndex* x)))
        [min-r min-c] (rowcol (reduce min global-idx-coll))
        [max-r max-c] (rowcol (reduce max global-idx-coll))]
    {:topleft-rowcol [min-r min-c]
     :height (inc (- max-r min-r))
     :width  (inc (- max-c min-c))}))

(defn global-coll->window-coll
  [sres wmap idx-val]
  (map (fn [[idx val]]
         [(window-idx sres wmap (GlobalIndex* idx)) val])
       idx-val))

(defn create-window
  [window-coll & {:keys [nrows ncols] :or {nrows 0 ncols 0}}]
  {:pre [(> nrows 0) (> ncols 0)]
   :post [(not-any? nil? (last %))]}
  (let [len (* nrows ncols)]
    (partition ncols
               (util/sparse-expander nil window-coll :length len))))

(defn process-neighbors
  [sres wmap f window-mat]
  (let [win-col (map-indexed vector
                             (walk/windowed-function 1 f window-mat))]
    (map (fn [[idx val]]
           [(global-index "500" (WindowIndex* idx) wmap) val])
         win-col)))

(defbufferop [assign-vals [sres]]
  [tuples]
  (let [wmap (window-map sres (map first tuples))
        window-coll (global-coll->window-coll sres wmap tuples)
        window-mat (create-window
                    (sort-by first window-coll)
                    :nrows (wmap :height)
                    :ncols (wmap :width))
        my-sum (fn [& args] (i/sum args))]
    (process-neighbors sres wmap my-sum window-mat)))



(defn mine [sres]
  (let [w-src (window-attribute-src sm-src lg-src sres)]
    (<- [?group ?new-idx ?new-val]
        (w-src ?group ?idx ?val)
        (assign-vals [sres] ?idx ?val :> ?new-idx ?new-val))))

(defn filtered-mine []
  (let [mine-src (mine "500")]
    (??<- [?group ?idx ?val]
          (mine-src ?group ?idx ?val)
          (sm-src ?group ?h ?v ?s ?l _)
          (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
          (global-index "500" ?tile-pixel :> ?idx))))



