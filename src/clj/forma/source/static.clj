(ns forma.source.static
  (:use cascalog.api
        [forma.static :only (static-datasets)])
  (:require [cascalog.ops :as c]
            [forma.reproject :as r]
            [forma.schema :as schema]
            [forma.thrift :as thrift]
            [forma.hadoop.io :as io]
            [clojure.java.io :as java.io]
            [forma.hadoop.predicate :as p]))

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
  (with-open [reader (java.io/reader (str base-path old-file-name))
              writer (java.io/writer (str base-path new-file-name))]
    (binding [*out* writer]
      (doseq [line (->> reader
                        (map-indexed (fn [idx line]
                                       (str (- idx num-drop) " " line)))
                        (drop num-drop))]
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
    (<- [?dataset ?m-res ?t-res !date ?mod-h ?mod-v ?sample ?line ?val]
        (line-tap ?textline)
        (p/break ?textline :> ?row ?col ?val)
        (pixel-tap ?mod-h ?mod-v ?sample ?line)
        (r/wgs84-indexer m-res ascii-info ?mod-h ?mod-v ?sample ?line :> ?row ?col)
        (p/add-fields dataset m-res "00" nil :> ?dataset ?m-res ?t-res !date))))

(defn downsample-modis
  "Returns a cascalog query designed to pair each value on an ASCII
  data grid with a particular MODIS pixel at the specified spatial
  resolution, `m-res`. The modis pixels produced by the supplied
  `pixel-tap` act as a set; only pixels with locations matching those
  produced by `pixel-tap` will be output.

 `downsample-modis` handles situations in which the ASCII resolution
  is higher than the supplied MODIS resolution; more than one value is
  guaranteed to exist for every MODIS pixel. `agg` (`c/sum` or `c/max`
  are supported, currently) determines the way in which multiple
  values are combined."
  [m-res dataset pixel-tap line-tap agg]
  {:pre [(#{c/sum, c/max} agg)]}
  (let [ascii-info (static-datasets (keyword dataset))]
    (<- [?dataset ?m-res ?t-res !date ?mod-h ?mod-v ?sample ?line ?val]
        (line-tap ?textline)
        (p/break ?textline :> ?row ?col ?temp-val)
        (r/modis-indexer m-res ascii-info ?row ?col :> ?mod-h ?mod-v ?sample ?line)
        (pixel-tap ?mod-h ?mod-v ?sample ?line :> true)
        (agg ?temp-val :> ?val)
        (p/add-fields dataset m-res "00" nil :> ?dataset ?m-res ?t-res !date))))

;; TODO: Figure out a way to specify the projection we're coming from,
;; and relate it to the supplied m-res. currently, we just assume it's
;; 500m, given that "2" multiplier. The 5 down here is because the
;; initial mod-h, mod-v for the dataset was 0, 5.
(defn absorb-modis
  [m-res dataset pixel-tap line-tap agg]
  (let [ascii-info (static-datasets (keyword dataset))
        span (r/pixels-at-res m-res)]
    (<- [?dataset ?m-res ?t-res !date ?mod-h ?mod-v ?sample ?line ?val]
        (line-tap ?textline)
        (p/break ?textline :> ?row ?col ?val)
        ((c/juxt #'mod #'quot) ?col span :> ?sample ?mod-h)
        ((c/juxt #'mod #'quot) ?row span :> ?line ?mv)
        (pixel-tap ?mod-h ?mod-v ?sample ?line :> true)
        (+ 5 ?mv :> ?mod-v)
        (p/add-fields dataset m-res "00" nil :> ?dataset ?m-res ?t-res !date))))

;; TODO: Make a note that gzipped files can't be unpacked well when
;; they exist on S3. They need to be moved over to HDFS for that. I
;; think the answer here is to code up some sort of way to transfer
;; the textfile first, then open it up.
;;
;; Added to the wiki.

(defn agg-chunks
  "val-gen must generate

  ?dataset ?m-res ?t-res !date ?mod-h ?mod-v ?sample ?line ?val"
  [val-gen m-res chunk-size nodata]
  (let [src (p/sparse-windower val-gen
                               ["?sample" "?line"]
                               (r/chunk-dims m-res chunk-size)
                               "?val"
                               nodata)]
    (<- [?tile-chunk]
        (src ?dataset ?s-res ?t-res _ ?h ?v  _ ?id ?window)
        (p/flatten-window ?window :> ?data)
        (thrift/pack ?data :> ?val)
        (count ?data :> ?count)
        (= ?count chunk-size)
        (thrift/ModisChunkLocation* ?s-res ?h ?v ?id chunk-size :> ?tile-loc)
        (thrift/DataChunk* ?dataset ?tile-loc ?val ?t-res :> ?tile-chunk))))

(defn static-chunks
  "TODO: DOCS!"
  [m-res chunk-size dataset agg line-tap pix-tap]
  (let [upsample? (>= (-> dataset keyword static-datasets :step)
                      (r/wgs84-resolution m-res))]
    (-> (if upsample?
          (upsample-modis m-res dataset pix-tap line-tap)
          (downsample-modis m-res dataset pix-tap line-tap agg))
        (agg-chunks m-res chunk-size -9999))))

(defn static-modis-chunks
  "TODO: DESTROY. Replace with a better system."
  [chunk-size dataset agg line-tap pix-tap]
  (-> (absorb-modis "500" dataset pix-tap line-tap agg)
      (agg-chunks "500" chunk-size -9999)))
