(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
        [forma.hadoop.pail :only (?pail- split-chunk-tap)]
        [forma.source.tilesets :only (tile-set)]
        [cascalog.io :only (with-fs-tmp)]
        [forma.source.modis :only (pixels-at-res
                                   chunk-dims)]
        [forma.hadoop.predicate :only (sparse-windower
                                       pixel-generator)])
  (:require [forma.hadoop.predicate :as p]
            [forma.hadoop.io :as io]
            [forma.source.hdf :as h]
            [forma.source.rain :as r]
            [forma.source.fire :as f]
            [forma.source.static :as s]
            [forma.static :as static]
            [cascalog.ops :as c])
  (:gen-class))

;; And, here we are, at the chunkers. These simply execute the
;; relevant subqueries, using a TemplateTap designed to sink our
;; tuples into our custom directory structure.

(defn to-pail
  [pail-path query]
  (?pail- (split-chunk-tap pail-path)
      query))

(defn chunk-job
  [out-path query]
  (?- (io/chunk-tap out-path)))
       
(defn modis-chunker
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets chunk-size in-path pattern pail-path]
  (let [source (io/hfs-wholefile in-path :pattern pattern)]
    (to-pail pail-path
             (h/modis-chunks subsets chunk-size source))))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res chunk-size tile-seq path out-path]
  (with-fs-tmp [fs tmp-dir]
    (let [file-tap (io/hfs-wholefile path)
          pix-tap (p/pixel-generator tmp-dir m-res tile-seq)
          ascii-map (:precl static/static-datasets)]
      (to-pail out-path
               (r/rain-chunks m-res ascii-map chunk-size file-tap pix-tap)))))

(defn static-chunker
  [m-res chunk-size tile-seq dataset agg ascii-path out-path]
  (with-fs-tmp [fs tmp-dir]
    (let [line-tap (hfs-textline ascii-path)
          pix-tap  (p/pixel-generator tmp-dir m-res tile-seq)]
      (chunk-job out-path
                 (s/static-chunks m-res chunk-size dataset agg line-tap pix-tap)))))

;; Note that the preprocessing performed by `fire-chunker` is going to
;; aggregate fires into daily buckets; we won't be able to get any
;; other information. We should probably change this in future to
;; retain as much information as possible.

(defn fire-chunker
  "Preprocessing for fires data."
  [m-res in-path pattern out-path]
  (let [source (hfs-textline in-path :pattern pattern)]
    (chunk-job out-path
               (f/reproject-fires m-res source))))

(defn modis-main
  "See project wiki for example usage."
  [path output-path & tiles]
  (let [pattern (->> tiles
                     (map read-string)
                     (apply tile-set)
                     (apply io/tiles->globstring))]
    (modis-chunker static/forma-subsets
                   static/chunk-size
                   path
                   pattern
                   output-path)))

(defn rain-main
  "See project wiki for example usage."
  [path output-path & countries]
  (let [countries (map read-string countries)]
    (rain-chunker "1000"
                  static/chunk-size
                  (apply tile-set countries)
                  path
                  output-path)))

(defn static-main
  "See project wiki for example usage."
  [dataset ascii-path output-path & countries]
  (static-chunker "1000"
                  static/chunk-size
                  (->> countries
                       (map read-string)
                       (apply tile-set))
                  dataset
                  c/sum
                  ascii-path
                  output-path))

(defn fire-main
  "See project wiki for example usage."
  [in-path pattern output-path]
  (fire-chunker "1000"
                in-path
                pattern
                output-path))

;;TODO: call read-string on the args. Abstract out from above
;;functions.
(defn -main
  "Wrapper to allow for various calls to chunking functions."
  [func-name & args]
  (let [func (case func-name
                   "modis" modis-main
                   "rain" rain-main
                   "fire" fire-main
                   "static" static-main)]
    (apply func args)))
