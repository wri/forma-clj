(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
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

(defn chunk-job
  [out-path query]
  (?- (io/chunk-tap out-path)
      query))

(defn modis-chunker
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets chunk-size pattern out-path]
  (let [source (io/globhfs-wholefile pattern)]
    (chunk-job out-path
               (h/modis-chunks subsets chunk-size source))))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res chunk-size tile-seq path out-path]
  (with-fs-tmp [fs tmp-dir]
    (let [file-tap (io/hfs-wholefile path)
          pix-tap (p/pixel-generator tmp-dir m-res tile-seq)
          ascii-map (:precl static/static-datasets)]
      (chunk-job out-path
                 (r/rain-chunks m-res ascii-map chunk-size file-tap pix-tap)))))

(defn static-chunker
  "TODO: DOCS!"
  [m-res chunk-size tile-seq dataset agg ascii-path out-path]
  (with-fs-tmp [fs tmp-dir]
    (let [line-tap (hfs-textline ascii-path)
          pix-tap  (p/pixel-generator tmp-dir m-res tile-seq)]
      (chunk-job out-path
                 (s/static-chunks m-res chunk-size dataset agg line-tap pix-tap)))))

;; TODO: DOCUMENT. We should note that this is going to get bucketed
;; at daily temporal resolution. The static datasets are going to get
;; bucketed at `00`.
(defn fire-chunker
  "Preprocessing for fires data."
  [m-res pattern out-path]
  (let [source (io/globhfs-textline pattern)]
    (chunk-job out-path
               (f/reproject-fires m-res source))))

(defn modis-main
  "Example usage:

   hadoop jar forma-standalone.jar forma.hadoop.jobs.preprocess
          modis modisdata/MOD13A3/ reddoutput/ [8 6] [10 12]"
  [path output-path & tiles]
  (let [tileseq (map read-string tiles)
        pattern (str path (apply io/tiles->globstring tileseq))]
    (modis-chunker static/forma-subsets
                   static/chunk-size
                   pattern
                   output-path)))

(defn rain-main
  "TODO: Example usage."
  [path output-path & countries]
  (let [countries (map read-string countries)]
    (rain-chunker "1000"
                  static/chunk-size
                  (apply tile-set countries)
                  path
                  output-path)))

(defn static-main
  "TODO: Example usage."
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
  "TODO: Example usage."
  [pattern output-path]
  (fire-chunker "1000"
                pattern
                output-path))

;;TODO: call read-string on the args.
(defn -main
  "Wrapper to allow for various calls to chunking functions."
  [func-name & args]
  (let [func (case func-name
                   "modis" modis-main
                   "rain" rain-main
                   "fire" fire-main
                   "static" static-main)]
    (apply func args)))
