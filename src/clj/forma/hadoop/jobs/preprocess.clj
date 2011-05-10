(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
        [forma.source.modis :only (valid-modis?)]
        [forma.hadoop.io :only (chunk-tap
                                hfs-wholefile
                                globhfs-wholefile
                                globhfs-textline)])
  (:require [forma.source.hdf :as h]
            [forma.source.rain :as r]
            [forma.source.fire :as f]
            [forma.static :as s])
  (:gen-class))

;; TEMPORARY stuff for the big run :-*
;;
;; TODO: move to hadoop.io

(defn tiles->globstring
  [& tiles]
  {:pre [(valid-modis? tiles)]}
  (let [hv-seq (interpose "," (for [[th tv] tiles]
                                (format "h%02dv%02d" th tv)))]
    (format "*{%s}*" (apply str hv-seq))))

(defn s3-path [path]
  (str "s3n://AKIAJ56QWQ45GBJELGQA:6L7JV5+qJ9yXz1E30e3qmm4Yf7E1Xs4pVhuEL8LV@" path))

;; And, here we are, at the chunkers. These simply execute the
;; relevant subqueries, using a TemplateTap designed to sink our
;; tuples into our custom directory structure.

(defn modis-chunker
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets chunk-size pattern out-path]
  (let [source (globhfs-wholefile pattern)]
    (?- (chunk-tap out-path)
        (h/modis-chunks subsets chunk-size source))))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res ll-res chunk-size tile-seq path out-path]
  (let [source (hfs-wholefile path)]
    (?- (chunk-tap out-path)
        (r/rain-chunks m-res ll-res chunk-size tile-seq source))))

;; TODO: DOCUMENT. We should note that this is going to get bucketed
;; at daily temporal resolution. The static datasets are going to get
;; bucketed at `00`.
(defn fire-chunker
  "Preprocessing for fires data."
  [m-res pattern out-path]
  (let [source (globhfs-textline pattern)]
    (?- (chunk-tap out-path)
        (f/reproject-fires m-res source))))

(defn modis-main
  "Example usage:

   hadoop jar forma-standalone.jar modis modisdata/MOD13A3/ reddoutput/ [8 6] [10 12]"
  [path output-path & tiles]
  (let [tileseq (map read-string tiles)
        pattern (str path (apply tiles->globstring tileseq))]
    (modis-chunker s/forma-subsets
                   s/chunk-size
                   (s3-path pattern)
                   (s3-path output-path))))

(defn rain-main
  "TODO: Example usage."
  [path output-path & tiles]
  (rain-chunker "1000"
                0.5
                s/chunk-size
                (map read-string tiles)
                (s3-path path)
                (s3-path output-path)))

(defn fire-main
  "TODO: Example usage."
  [pattern output-path]
  (fire-chunker "1000"
                (s3-path pattern)
                (s3-path output-path)))

(defn -main
  "Wrapper to allow for various calls to chunking functions."
  [func-name & args]
  (let [func (case func-name
                   "modis" modis-main
                   "rain" rain-main
                   "fire" fire-main)]
    (apply func args)))
