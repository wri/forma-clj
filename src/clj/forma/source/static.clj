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

;; TODO: UPDATE DOCS
(defn upsample-modis
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
  [m-res dataset pixel-tap line-tap]
  (let [ascii-info (static-datasets (keyword dataset))]
    (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?outval]
        (line-tap ?line)
        (p/break ?line :> ?row ?col ?outval)
        (pixel-tap ?mod-h ?mod-v ?sample ?line)
        (wgs84-indexer m-res ascii-info ?mod-h ?mod-v ?sample ?line :> ?row ?col)
        (m/hv->tilestring ?mod-h ?mod-v :> ?tilestring)
        (p/add-fields dataset m-res "00" :> ?dataset ?m-res ?t-res))))

;; TODO: UPDATE DOCS
(defn downsample-modis
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
  [m-res dataset line-tap agg]
  {:pre [(or (= agg c/sum) (= agg c/max))]}
  (let [ascii-info (static-datasets (keyword dataset))]
    (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?outval]
        (line-tap ?line)
        (p/break ?line :> ?row ?col ?val)
        (modis-indexer m-res ascii-info ?row ?col :> ?mod-h ?mod-v ?sample ?line)
        (agg ?val :> ?outval)
        (p/add-fields dataset m-res "00" :> ?dataset ?m-res ?t-res)
        (m/hv->tilestring ?mod-h ?mod-v :> ?tilestring))))

(defn static-chunks
  "TODO: DOCS!"
  [m-res chunk-size dataset agg line-tap pix-tap]
  (let [[width height] (m/chunk-dims m-res chunk-size)
        upsample? (>= (-> dataset keyword static-datasets :step)
                      (wgs84-resolution m-res))
        window-src (-> (if upsample?
                         (upsample-modis m-res dataset pix-tap line-tap)
                         (downsample-modis m-res dataset line-tap agg))
                       (p/sparse-windower ["?sample" "?line"]
                                          [width height]
                                          "?outval"
                                          -9999))]
    (<- [?dataset ?spatial-res ?t-res ?tilestring ?chunkid ?chunk]
        (window-src ?dataset ?spatial-res ?t-res ?tilestring  _ ?chunkid ?window)
        (p/window->struct [:double] ?window :> ?chunk))))
