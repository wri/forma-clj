(ns forma.source.static
  (:use cascalog.api
        [forma.matrix.utils :only (idx->colrow)]
        [forma.reproject :only (wgs84-index
                                dimensions-at-res
                                modis-sample)]
        [forma.source.modis :only (modis->latlon latlon->modis pixels-at-res)]
        [forma.source.static-test]
        [clojure.string :only (split)]
        [clojure.contrib.duck-streams :only (read-lines with-out-writer)])
  (:require [cascalog.ops :as c]))

;; Used for testing
(def ascii-path "/Users/danhammer/Desktop/grid.txt")
(def ascii-info {:ncols 3600 :nrows 1737 :xulcorner -180 :yulcorner 83.7 :cellsize 0.1 :nodata -9999})

;; Worth keeping

(defn index-textfile
  "Prepend a row index to a text file `old-file-name`, located at `base-path`,
  and save in the same directory with new name `new-file-name`. The `num-drop`
  parameter specifies the number of lines to drop from the beginning of the 
  text file.  Note that indexing begins with the first textline not dropped,
  and starts at 0. This function was originally written to clean an ASCII
  raster file for input into the sampling functions."
  [base-path old-file-name new-file-name num-drop]
  (let [old-file (str base-path old-file-name)
        new-file (str base-path new-file-name)]
    (with-out-writer new-file
      (doseq [line (drop num-drop
                         (map-indexed
                          (fn [idx line] (str (- idx num-drop) " " line))
                          (read-lines old-file)))]
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

(defmapcatop
  ^{:doc "split a row into nested vectors, with the value as the second entry
  and the column index (from 0) as the first entry.  Note that this is
  particularly useful if the `row` had already been prepended with the
  row index -- which is not passed into `free-cols`"}
  free-cols
  [row]
  (map-indexed vector row))

(defn ascii-source [path]
  "create a source out of a pre-cleaned ASCII grid, which is associated with a
  map of characteristic values, such as resolution and extent.  The cleaned
  grid is located at `path` and has the row index values prepended on the rows,
  with the ASCII prelude (which is automatically added by Arc) already stripped
  and cataloged in the map of characteristic values."
  (let [source (hfs-textline path)]
    (<- [?row ?col ?val]
        (source ?line)
        (liberate ?line :> ?row ?row-vec)
        (free-cols ?row-vec :> ?col ?val))))

;; TODO: generalize this function, and its dependency functions, to
;; accomodate different projections.

(defn sample-modis
  "sample a series of points with an underlying ASCII grid, so that each point is
  tagged with the value of the grid pixel that it falls within. Note that, currently,
  this query requires that both the point array and grid be projected in WGS84."
  [mod-source ascii-path ascii-info]
  (let [grid-source (ascii-source ascii-path)]
    (?<- (stdout)
         [?mod-h ?mod-v ?line ?sample ?row ?col ?val]
         (grid-source ?row ?col ?val)
         (mod-source ?res ?mod-h ?mod-v ?line ?sample)
         (modis-sample ascii-info ?res ?mod-h ?mod-v ?sample ?line :> ?row ?col))))

