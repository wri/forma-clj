(ns forma.source.static
  (:use cascalog.api
        [forma.source.modis :only (wgs84-resolution)]
        [forma.static :only (static-datasets)]
        [forma.reproject :only (wgs84-indexer
                                rowcol->modis)]
        [clojure.string :only (split)])
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

(defn liberate
  "Takes a line with an index as the first value and numbers as the
  rest, and converts it into a 2-tuple formatted as `[idx, row-vals]`,
  where `row-vals` are sealed inside an `int-array`.

  Example usage:

    (liberate \"1 12 13 14 15\")
    ;=> [1 #<int[] [I@1b66e87>]"
  [line]
  (let [[idx & row-vals] (map #(Integer. %)
                              (split line #" "))]
    [idx (int-array row-vals)]))

(defn ascii-source
  "create a source out of a pre-cleaned ASCII grid, which is
  associated with a map of characteristic values, such as
  resolution. The cleaned grid is located at `path` and has the row
  index values prepended on the rows, with the ASCII
  prelude (automatically added by Arc) already stripped and catalogued
  in the map of characteristic values."
  [path]
  (let [source (hfs-textline path)]
    (<- [?row ?col ?val]
        (source ?line)
        (liberate ?line :> ?row ?row-vec)
        (p/index ?row-vec :> ?col ?val))))

(defn downsample
  "This query is for a point grid at higher resolution than the
  reference grid, which is the MODIS grid in this case. (This is a
  constraining assumption, since MODIS is hard-coded into this
  function.  The aggregator function should be called as follows:
  c/sum, c/max, where c is the prefix refering to cascalog.ops."
  [dataset m-res ascii-info grid-source agg]
  (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?outval]
      (grid-source ?row ?col ?val)
      (identity dataset :> ?dataset)
      (identity "00" :> ?t-res)
      (agg ?val :> ?outval)
      (rowcol->modis m-res ascii-info ?row ?col :> ?mod-h ?mod-v ?sample ?line)))

(defn upsample
  "sample values off of a lower resolution grid than the MODIS grid."
  [dataset m-res ascii-info grid-source pixel-tap]
  (let [{:keys [step corner travel]} ascii-info
        [lon0 lat0] corner
        [lon-dir lat-dir] travel
        indexer (wgs84-indexer m-res step lat-dir lon-dir lat0 lon0)]
    (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?outval]
        (grid-source ?row ?col ?outval)
        (identity dataset :> ?dataset)
        (identity "00" :> ?t-res)
        (pixel-tap ?mod-h ?mod-v ?sample ?line)
        (indexer ?mod-h ?mod-v ?sample ?line :> ?row ?col))))

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
  ([m-res dataset pixel-tap ascii-path & [agg]]
     (let [ascii-info ((keyword dataset) static-datasets)
           grid-source (ascii-source ascii-path)]
       (if (>= (:cellsize ascii-info)
               (wgs84-resolution m-res))
         (upsample dataset m-res ascii-info grid-source pixel-tap)
         (downsample dataset m-res ascii-info grid-source agg)))))

(defn tilestringer
  "TODO: DOCS"
  [src]
  (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?val]
      (src ?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?val)
      (m/hv->tilestring ?mod-h ?mod-v :> ?tilestring)))
