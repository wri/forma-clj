(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
        [forma.source.modis :only (valid-modis?)]
        [forma.hadoop.io :only (chunk-tap
                                wholefile-tap
                                globbed-wholefile-tap)])
  (:require [forma.source.hdf :as h]
            [forma.source.rain :as r]
            [forma.source.modis :as m]
            [forma.static :as s])
  (:gen-class))

;; And, here we are, at the chunkers. These simply execute the
;; relevant subqueries, using a TemplateTap designed to sink our
;; tuples into our custom directory structure.

(defn modis-chunker
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets chunk-size in-dir out-dir]
  (let [source (globbed-wholefile-tap in-dir)]
    (?- (chunk-tap out-dir)
        (h/modis-chunks subsets chunk-size source))))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res ll-res chunk-size tile-seq in-dir out-dir]
  (let [source (wholefile-tap in-dir)]
    (?- (chunk-tap out-dir)
        (r/rain-chunks m-res ll-res chunk-size tile-seq source))))

;; TEMPORARY stuff for the big run :-*
(defn tiles->globstring
  [& tiles]
  {:pre [(m/valid-modis? tiles)]}
  (let [hv-seq (interpose "," (for [[th tv] tiles]
                                (format "h%02dv%02d" th tv)))]
    (format "*{%s}*" (apply str hv-seq))))

(defn s3-path [path]
  (str "s3n://AKIAJ56QWQ45GBJELGQA:6L7JV5+qJ9yXz1E30e3qmm4Yf7E1Xs4pVhuEL8LV@" path))

(defn chunk-main
  "Example usage:

   hadoop jar forma-standalone.jar modisdata/MOD13A3/ reddoutput/ [8 6] [10 12]"
  [input-path output-path & tiles]
  {:pre [(m/valid-modis? tiles)]}
  (let [tileseq (map read-string tiles)
        tilestring (str input-path (apply tiles->globstring tileseq))]
    (modis-chunker s/forma-subsets s/chunk-size
                   (s3-path input-path)
                   (s3-path output-path))))

(defn rain-main
  "TODO: Example usage."
  [input-path output-path & tiles]
  {:pre [(m/valid-modis? tiles)]}
  (rain-chunker "1000" 0.5 s/chunk-size (map read-string tiles)
                (s3-path input-path)
                (s3-path output-path)))

(defn -main
  "Wrapper to allow for various calls to chunking functions."
  [func-name & args]
  (apply (case func-name
                   "modis" modis-chunker
                   "rain" rain-chunker)
         args))
