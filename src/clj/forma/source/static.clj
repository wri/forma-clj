(ns forma.source.static
  (:use cascalog.api
        [forma.matrix.utils :only (idx->colrow)]
        [forma.hadoop.io :only (chunk-tap)]
        [forma.hadoop.predicate :only (sparse-windower)]
        [forma.reproject :only (wgs84-index
                                dimensions-at-res
                                modis-sample)]
        [forma.source.modis :only (modis->latlon
                                   latlon->modis
                                   pixels-at-res
                                   hv->tilestring)]
        [clojure.string :only (split)])
  (:require [cascalog.ops :as c]
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

(defn mod-pixels
  [t-seq]
  (memory-source-tap
   (for [[mod-h mod-v] t-seq
         sample 1200
         line   1200]
     ["1000" mod-h mod-v sample line])))

;; TODO: generalize this function, and its dependency functions, to
;; accomodate different projections.

(defn sample-modis
  "sample a series of points with an underlying ASCII grid, so that each point is
  tagged with the value of the grid pixel that it falls within. Note that, currently,
  this query requires that both the point array and grid be projected in WGS84."
  [tile-seq dataset ascii-path]
  (let [ascii-info ((keyword dataset) datasets)
        mod-source (mod-pixels tile-seq)
        grid-source (ascii-source ascii-path)]
    (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?val]
        (grid-source ?row ?col ?val)
        (identity dataset :> ?dataset)
        (identity "00" :> ?t-res)
        (mod-source ?res ?mod-h ?mod-v ?sample ?line)
        (modis-sample ascii-info ?m-res ?mod-h ?mod-v ?sample ?line :> ?row ?col))))

(defn tilestringer
  [src]
  (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?val]
      (src ?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?val)
      (hv->tilestring ?mod-h ?mod-v :> ?tilestring)))

(defn static-chunker
  "Last thing we need is the chunk-tap!"
  [tile-seq dataset ascii-path output-path]
  (let [src (sample-modis tile-seq dataset ascii-path)]
    (?- (chunk-tap output-path)
        (sparse-windower (tilestringer src) ["?sample" "?line" "?val"] 150 0))))

(defn s3-path [path]
  (str "s3n://AKIAJ56QWQ45GBJELGQA:6L7JV5+qJ9yXz1E30e3qmm4Yf7E1Xs4pVhuEL8LV@" path))

(defn -main
  [tile-seq dataset ascii-path output-path]
  (sample-modis (read-string tile-seq)
                dataset
                ascii-path
                (s3-path output-path)))


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
   [8001 11001 23]
   [8001 11002 45]
   [8003 11003 67]
   [8004 11004 89]
   [8005 11005 1011]
   [8006 11006 1213]
   [8007 11007 1415]
   [8008 11008 1617]
   [8009 11009 1819]
   [8010 11010 2021]
   [8011 11011 2223]
   ])

(def ascii-info {:ncols 36001
                 :nrows 13962
                 :xulcorner -180.000001
                 :yulcorner 83.635972
                 :cellsize 0.01
                 :nodata -9999})

(defn travel [step dir start pos]
  (-> start (dir (* pos step)) (dir (/ step 2))))

(defn rowcol->latlon
  [ascii-info row col]
  (let [csz (:cellsize ascii-info)
        [yul xul] (map ascii-info [:yulcorner :xulcorner])]
    (map (partial travel csz)
     [- +]
     [yul xul]
     [row col])))

(defn colval->modis
  "first convert row col into lat lon, and then convert lat lon
  into modis characteristics."
  [ascii-info row col]
  (rowcol->latlon ascii-info row col))

(defn casc-test-high-res
  [ascii-info]
  (?<- (stdout)
       [?row ?col ?val ?lat ?lon]
       (high-res-source-tap ?row ?col ?val)
       (rowcol->latlon ascii-info ?row ?col :> ?lat ?lon)
       #_(< ?col 11009)
       #_(c/sum ?val :> ?sum)))


