(ns forma.hadoop.jobs.neighbors
  (:use cascalog.api
        forma.hoptree)
  (:require [cascalog.ops :as c]
            [forma.matrix.utils :as util]
            [forma.matrix.walk :as walk]
            [incanter.core :as i]))

(defmapcatop neighbors
  "accepts a spatial resolution and a unique, global index and returns
  a sequence of the indices of the 8 adjacent pixels, as well as the
  own-pixel index."
  [sres idx]
  (neighbor-idx sres (GlobalIndex* idx)))

(defn neighbor-src
  "returns a tap with the unique, global pixel IDs of all neighbors
  that are relevant for neighbor analysis for a group of pixels."
  [select-src sres]
  (<- [?neigh-id]
      (select-src ?gadm ?h ?v ?s ?l)
      (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
      (global-index sres ?tile-pixel :> ?idx)
      (neighbors sres ?idx :> ?neigh-id)
      (:distinct true)))

(defn window-attribute-src
  "accepts two sources (1) coordinates and group ID of a selection of
  pixels that are to be assigned neighbor values; (2) all pixels in
  the region that contains the groups. Returns the selection of pixels
  and their values that are relevant for neighbor processing by
  screening out all unneccessary pixels."
  [select-src full-src sres]
  (let [n-src (neighbor-src select-src sres)]
    (<- [?gadm ?idx ?val]
        (n-src ?idx)
        (full-src ?gadm ?h ?v ?s ?l ?val)
        (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
        (global-index sres ?tile-pixel :> ?idx)
        (:distinct true))))

(defn window-map
  "accepts a spatial resolution (as a string) and a collection of
  global, unique indices and returns a defining definition of the
  rectangular window that bounds all pixels in the collection.  The
  window is fully identified by the topleft corner (in the form of
  [row column]), along with the width and height in pixels of the
  rectangular window."
  [sres global-idx-coll]
  (let [rowcol (fn [x] (global-rowcol sres (GlobalIndex* x)))
        [min-r min-c] (rowcol (reduce min global-idx-coll))
        [max-r max-c] (rowcol (reduce max global-idx-coll))]
    {:topleft-rowcol [min-r min-c]
     :height (inc (- max-r min-r))
     :width  (inc (- max-c min-c))}))

(defn global-coll->window-coll
  "accepts a spatial resolution, a map that identifies a MODIS window,
  and a sequence of tuples of the form [idx val], where the index is
  the unique global index.  Returns the same form, except where the
  index has been transformed to the relative window index."
  [sres wmap idx-val]
  {:pre [(contains?  wmap :topleft-rowcol)]}
  (map (fn [[idx val]] [(window-idx sres wmap (GlobalIndex* idx)) val])
       idx-val))

(defn create-window
  "acceots a sequence of tuples of the form [idx val], where idx is
  the unique identifier of the pixel within a designated window."
  [window-coll & {:keys [nrows ncols] :or {nrows 0 ncols 0}}]
  {:pre  [(> nrows 0) (> ncols 0)
          (contains? (vec (map first window-coll)) 0)]}
  (let [len (* nrows ncols)]
    (partition ncols
               (util/sparse-expander nil window-coll :length len))))

(defn process-neighbors
  "accepts a spatial resolution, a map of window parameters, a
  function to apply over a 9-pixel square of neighbors, and a
  rectangular matrix of pixel-level values.  Returns a sequence of
  tuples of the form [idx val] where the index is the global, unique
  MODIS index and the value is the transformed values as a result of
  spatial smoothing."
  [sres wmap f window-mat]
  (let [win-col (map-indexed vector
                             (walk/windowed-function 1 f window-mat))]
    (map (fn [[idx val]] [(global-index "500" (WindowIndex* idx) wmap) val])
         win-col)))

(defbufferop [assign-vals [sres]]
  "accepts a sequence of tuples within a `?group` field and returns a
  sequence of tuples with fields `global-index` and `smoothed-value`"
  [tuples]
  (let [wmap (window-map sres (map first tuples))
        window-coll (global-coll->window-coll sres wmap tuples)
        window-mat (create-window
                    (sort-by first window-coll)
                    :nrows (wmap :height)
                    :ncols (wmap :width))
        my-sum (fn [& args] (i/sum args))]
    (process-neighbors sres wmap my-sum window-mat)))

(defn sample-query
  "accepts a spatial resolution; a selection of pixels for which
  neighborhood attributes are required; and a full selection of
  indices and values for the region that contains all the group-level
  pixels.  Returns a tap with the transformed values assigned to the
  full set of indices."
  [sres select-src full-src]
  (let [w-src (window-attribute-src select-src full-src sres)]
    (<- [?group ?new-idx ?new-val]
        (w-src ?group ?idx ?val)
        (assign-vals [sres] ?idx ?val :> ?new-idx ?new-val))))

(defn filtered-sample-query
  "filters the `sample-query` return values to the indices in the
  selected pixel source."
  [sres select-src full-src]
  (let [src (sample-query sres select-src full-src)]
    (<- [?group ?idx ?val]
        (src ?group ?idx ?val)
        (select-src ?group ?h ?v ?s ?l)
        (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
        (global-index "500" ?tile-pixel :> ?idx))))
