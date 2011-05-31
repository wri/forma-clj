(ns forma.source.static
  (:use cascalog.api
        [forma.source.modis :only (wgs84-resolution)]
        [forma.static :only (static-datasets)]
        [forma.reproject :only (wgs84-indexer
                                modis-indexer)])
  (:require [cascalog.ops :as c]
            [forma.source.modis :as m]
            [forma.hadoop.predicate :as p]
            [clojure.contrib.duck-streams :as duck]))

;; ### Preprocessing
;;
;; Hadoop doesn't have the ability to find the line number of a
;; specific line within a textfile; before working with ASCII grids
;; out of ArcGIS, we need to manually process each file, adding a line
;; number to the beginning of each line. This gives us the ability to
;; retrieve the row index.

(defn index-textfile
  "Prepend a row index to a text file `old-file-name`, located at `base-path`,
  and save in the same directory with new name `new-file-name`. The
  `num-drop` parameter specifies the number of lines to drop from the
  beginning of the text file.  Note that indexing begins with the
  first textline not dropped, and starts at 0. This function was
  originally written to clean an ASCII raster file for input into the
  sampling functions."
  [base-path old-file-name new-file-name num-drop]
  (let [old-file (str base-path old-file-name)
        new-file (str base-path new-file-name)]
    (duck/with-out-writer new-file
      (doseq [line (drop num-drop
                         (map-indexed
                          (fn [idx line] (str (- idx num-drop) " " line))
                          (duck/read-lines old-file)))]
        (println line)))))

;; ### Sampling

;; TODO: Clean this fucker up! we need to get rid of that into
;; duplication.

(defn sample-modis
  "This function is based on the reference MODIS grid (currently at
  1000m res).  The objective of the function is to tag each MODIS
  pixel with a value from an input ASCII grid.  The process diverges
  based on whether the ASCII grid is at coarser or finer resolution
  than the MODIS grid.  If higher, then each ASCII point is assigned a
  unique MODIS identifier, and the duplicate values are aggregated
  based on an input aggregator (sum, max, etc.).  If lower, each MODIS
  pixel is tagged with the ASCII grid cell that it falls within.  Note
  that the ASCII grid must be in WGS84, with row indices prepended and
  the ASCII header lopped off."
  [m-res dataset pixel-tap line-tap agg]
  {:pre [(or (= agg c/sum) (= agg c/max))]}
  (let [ascii-info (static-datasets (keyword dataset))
        mod-coords ["?mod-h" "?mod-v" "?sample" "?line"]
        rc-coords ["?row" "?col"]
        upsample? (>= (:step ascii-info) (wgs84-resolution m-res))
        aggers (if upsample?
                 [[pixel-tap :>> mod-coords]
                  [wgs84-indexer :<< (into [m-res ascii-info] mod-coords) :>> rc-coords]]
                 [[modis-indexer :<< (into [m-res ascii-info] rc-coords) :>> mod-coords]])]
    (construct
     ["?dataset" "?m-res" "?t-res" "?tilestring" "?sample" "?line" "?outval"]
     (into aggers
           [[line-tap "?line"]
            [p/break "?line" :> "?row" "?col" "?val"]
            [agg "?val" :> "?outval"]
            [p/add-fields dataset "00" :> "?dataset" "?t-res"]
            [m/hv->tilestring "?mod-h" "?mod-v" :> "?tilestring"]]))))

(defn static-chunks
  "TODO: DOCS!"
  [m-res chunk-size dataset agg line-tap pix-tap]
  (let [[width height] (m/chunk-dims m-res chunk-size)
        window-src (-> (sample-modis m-res dataset pix-tap line-tap agg)
                       (p/sparse-windower ["?sample" "?line"]
                                          [width height]
                                          "?outval"
                                          -9999))]
    (<- [?dataset ?spatial-res ?t-res ?tilestring ?chunkid ?chunk]
        (window-src ?dataset ?spatial-res ?t-res ?tilestring  _ ?chunkid ?window)
        (p/window->array [Float/TYPE] ?window :> ?chunk))))
