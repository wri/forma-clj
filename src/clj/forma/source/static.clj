(ns forma.source.static
  (:use cascalog.api
        [forma.hadoop.io :only (chunk-tap)]        
        [forma.reproject :only (modis-sample)]
        [clojure.string :only (split)]
        [forma.hadoop.predicate :only (sparse-windower
                                       pixel-generator)])
  (:require [cascalog.ops :as c]
            [forma.source.modis :as m]
            [cascalog.io :as io]
            [clojure.contrib.duck-streams :as duck])
  (:gen-class))

(def datasets {:gadm {:ncols 36001
                      :nrows 13962
                      :xulcorner -180.000001
                      :yulcorner 83.635972
                      :cellsize 0.01
                      :nodata -9999}
               :ecoid {:ncols 36000
                       :nrows 17352
                       :xulcorner -179.99996728576
                       :yulcorner 83.628027
                       :cellsize 0.01
                       :nodata -9999}
               :hansen {:ncols 86223
                        :nrows 19240
                        :xulcorner -179.99998844516
                        :yulcorner 40.164567
                        :cellsize 0.0041752289295106
                        :nodata -9999}
               :vcf {:ncols 86223
                     :nrows 19240
                     :xulcorner -179.99998844516
                     :yulcorner 40.164567
                     :cellsize 0.0041752289295106
                     :nodata -9999}})

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

;; This is testing space for DAN! and it will probably be erased
;; before we merge this feature branch.  I am trying to figure out a
;; way to sample datasets at higher resolution than the MODIS base
;; grid.

(defn high-res-ascii-source
  [path aggregator]
  (let [source (hfs-textline path)]
    (<- [?mod-h ?mod-v ?line ?sample ?row ?col ?val]
        (source ?line)
        (liberate ?line :> ?row ?row-vec)
        (free-cols ?row-vec :> ?col ?val)
        #_(colval->modis ascii-info ?row ?col :> ?mod-h ?mod-v ?line ?sample))))


(def high-res-source-tap
  [
   [8000 11000 01]
   [8001 11000 23]
   [8002 11000 45]
   [8003 11000 67]
   [8004 11000 89]
   [8005 11000 1011]
   [8006 11001 1213]
   [8007 11001 1415]
   [8008 11001 1617]
   [8009 11001 1819]
   [8010 11001 2021]
   [8011 11001 2223]
   ])

(defn travel
  "travel along a grid with cellsize `step` in the direction
  given by `dir` from an initial position `start` to a position
  `pos` which is intended to be, for most applications, row or
  column within the grid.  Note that this takes you to the
  centroid of the row or column position that you specify."
  [step dir start pos]
  (-> start (dir (* pos step)) (dir (/ step 2))))

(defn rowcol->latlon
  "Given an ASCII header map, find the latlon of the centroid of
  a cell given by `row` and `col`."
  [ascii-info row col]
  (let [csz (:cellsize ascii-info)
        [yul xul] (map ascii-info [:yulcorner :xulcorner])]
    (map (partial travel csz)
     [- +]
     [yul xul]
     [row col])))

(defn colval->modis
  "first convert row col into lat lon, and then convert lat lon
  into modis characteristics at resolution `m-res`."
  [m-res ascii-info row col]
  (->> (rowcol->latlon ascii-info row col)
       (apply m/latlon->modis m-res)))

(defn high-res-sample
  "This query is for a point grid at higher resolution than the reference
  grid, which is the MODIS grid in this case. (This is a constraining
  assumption, since MODIS is hard-coded into this function.  The aggregator
  function should be called as follows: c/sum, c/max, where c is the prefix
  refering to cascalog.ops."
  [dataset m-res grid-source ascii-info agg]
  (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?outval]
      (grid-source ?row ?col ?val)
      (identity dataset :> ?dataset)
      (identity "00" :> ?t-res)
      (agg ?val :> ?outval)
      (colval->modis m-res ascii-info ?row ?col :> ?mod-h ?mod-v ?sample ?line)))

(defn low-res-sample
  "sample values off of a lower resolution grid than the MODIS grid."
  [dataset m-res ascii-info pixel-tap grid-source]
  (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?outval]
      (grid-source ?row ?col ?outval)
      (identity dataset :> ?dataset)
      (identity "00" :> ?t-res)
      (pixel-tap ?mod-h ?mod-v ?sample ?line)
      (modis-sample ascii-info m-res ?mod-h ?mod-v ?sample ?line :> ?row ?col)))

;; TODO: Right now, we use the 0.1 as an indicator for conversion
;; direction, between modis and wgs84. Replace with a calculation.

(defn sample-modis
  "This function is based on the reference MODIS grid (currently at 1000m res).  The
  objective of the function is to tag each MODIS pixel with a value from an input
  ASCII grid.  The process diverges based on whether the ASCII grid is at coarser or
  finer resolution than the MODIS grid.  If higher, then each ASCII point is assigned
  a unique MODIS identifier, and the duplicate values are aggregated based on an input
  aggregator (sum, max, etc.).  If lower, each MODIS pixel is tagged with the ASCII
  grid cell that it falls within.  Note that the ASCII grid must be in WGS84, with row
  indices prepended and the ASCII header lopped off."
  ([m-res dataset pixel-tap ascii-path & [agg]]
     (let [ascii-info ((keyword dataset) datasets)
           grid-source (ascii-source ascii-path)]
       (if (>= (:cellsize ascii-info) 0.01)
         (low-res-sample dataset m-res ascii-info pixel-tap grid-source)
         (high-res-sample dataset m-res grid-source ascii-info agg)))))

;; THE JOB!

(defn tilestringer
  [src]
  (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?val]
      (src ?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?val)
      (m/hv->tilestring ?mod-h ?mod-v :> ?tilestring)))

(defn static-chunker
  "Last thing we need is the chunk-tap!"
  [m-res tile-seq dataset agg ascii-path output-path]
  (io/with-fs-tmp [fs tmp-dir]
    (let [pix-tap (pixel-generator tmp-dir m-res tile-seq)
          src (sample-modis m-res dataset pix-tap ascii-path agg)]
      (?- (chunk-tap output-path)
          (sparse-windower (tilestringer src) ["?sample" "?line"] "?val" 1200 20 0)))))

(defn s3-path [path]
  (str "s3n://AKIAJ56QWQ45GBJELGQA:6L7JV5+qJ9yXz1E30e3qmm4Yf7E1Xs4pVhuEL8LV@" path))

(def country-tiles [[27 7] [27 8] [27 9] [28 7] [28 8] [28 9] [29 8] [29 9] [30 8] [30 9] [31 8] [31 9]])

(defn -main
  [dataset ascii-path output-path]
  (static-chunker "1000"
                  country-tiles
                  dataset
                  c/sum
                  ascii-path
                  (s3-path output-path)))
