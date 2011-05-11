(ns forma.source.static
  (:use cascalog.api
        [forma.source.modis :only (wgs84-resolution)]
        [forma.static :only (static-datasets)]
        [forma.reproject :only (wgs84-indexer
                                modis-indexer)])
  (:require [forma.source.modis :as m]
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

(defn downsample
  "This query is for a point grid at higher resolution than the
  reference grid, which is the MODIS grid in this case. (This is a
  constraining assumption, since MODIS is hard-coded into this
  function.  The aggregator function must be either `cascalop.ops/sum`
  or `cascalog.ops/max`."
  [dataset m-res ascii-info line-tap agg]
  (let [indexer (modis-indexer m-res ascii-info)]
    (construct ["?dataset" "?m-res" "?t-res" "?tilestring" "?sample" "?line" "?outval"]
               [[line-tap "?line"]
                [p/break "?line" :> "?row" "?col" "?val"]
                [p/add-fields dataset "00" :> "?dataset" "?t-res"]
                [m/hv->tilestring "?mod-h" "?mod-v" :> "?tilestring"]
                [agg "?val" :> "?outval"]
                [indexer "?row" "?col" :> "?mod-h" "?mod-v" "?sample" "?line"]])))

(defn upsample
  "sample values off of a lower resolution grid than the MODIS grid."
  [dataset m-res ascii-info line-tap pixel-tap]
  (let [indexer (wgs84-indexer m-res ascii-info)]
    (construct ["?dataset" "?m-res" "?t-res" "?tilestring" "?sample" "?line" "?outval"]
               [[line-tap "?line"]
                [p/break "?line" :> "?row" "?col" "?outval"]
                [p/add-fields dataset "00" :> "?dataset" "?t-res"]
                [m/hv->tilestring "?mod-h" "?mod-v" :> "?tilestring"]
                [pixel-tap "?mod-h" "?mod-v" "?sample" "?line"]
                [indexer "?mod-h" "?mod-v" "?sample" "?line" :> "?row" "?col"]])))

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
  ([m-res dataset pixel-tap line-tap & [agg]]
     (let [ascii-info (static-datasets (keyword dataset))]
       (if (>= (:cellsize ascii-info)
               (wgs84-resolution m-res))
         (upsample   dataset m-res ascii-info line-tap pixel-tap)
         (downsample dataset m-res ascii-info line-tap agg)))))
