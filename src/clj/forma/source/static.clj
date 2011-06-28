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

(defn upsample-modis
  "Returns a cascalog query designed to pair each value on an ASCII
  data grid with a particular MODIS pixel at the specified spatial
  resolution, `m-res`.

 `upsample-modis` handles the case in which the resolution of the
  supplied ASCII grid is lower than the supplied MODIS resolution. We
  accept a source of MODIS coordinates along with the source of text
  lines, determine the position within the ascii grid of the centroid
  of each modis coordinate, and perform an inner join on the ASCII
  data, effectively sampling all ASCII data into a higher resolution."
  [m-res dataset pixel-tap line-tap]
  (let [ascii-info (static-datasets (keyword dataset))]
    (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?outval]
        (line-tap ?textline)
        (p/break ?textline :> ?row ?col ?outval)
        (pixel-tap ?mod-h ?mod-v ?sample ?line)
        (wgs84-indexer m-res ascii-info ?mod-h ?mod-v ?sample ?line :> ?row ?col)
        (m/hv->tilestring ?mod-h ?mod-v :> ?tilestring)
        (p/add-fields dataset m-res "00" :> ?dataset ?m-res ?t-res))))

(defn downsample-modis
  "Returns a cascalog query designed to pair each value on an ASCII
  data grid with a particular MODIS pixel at the specified spatial
  resolution, `m-res`.

 `downsample-modis` handles situations in which the ASCII resolution
  is higher than the supplied MODIS resolution; more than one value is
  guaranteed to exist for every MODIS pixel. `agg` (`c/sum` or `c/max`
  are supported, currently) determines the way in which multiple
  values are combined."  [m-res dataset line-tap agg] {:pre [(or (=
  agg c/sum) (= agg c/max))]}
  (let [ascii-info (static-datasets (keyword dataset))]
    (<- [?dataset ?m-res ?t-res ?tilestring ?sample ?line ?outval]
        (line-tap ?textline)
        (p/break ?textline :> ?row ?col ?val)
        (modis-indexer m-res ascii-info ?row ?col :> ?mod-h ?mod-v ?sample ?line)
        (agg ?val :> ?outval)
        (p/add-fields dataset m-res "00" :> ?dataset ?m-res ?t-res)
        (m/hv->tilestring ?mod-h ?mod-v :> ?tilestring))))

;; TODO: Make a note that gzipped files can't be unpacked well when
;; they exist on S3. They need to be moved over to HDFS for that. I
;; think the answer here is to code up some sort of way to transfer
;; the textfile first, then open it up.
;;
;; Added to the wiki.

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
        (p/window->struct [:int] ?window :> ?chunk))))
