(ns forma.hadoop.jobs.chunk-modis
  (:use cascalog.api
        [forma.hadoop.io :only (chunk-tap
                                globbed-wholefile-tap)])
  (:require [forma.source.hdf :as h]
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

;; TEMPORARY stuff for the big run :-*
(defn tiles->globstring
  [& tiles]
  {:pre [(not (some false? (map (fn [[h v]]
                                  (m/valid-modis? h v))
                                tiles)))]}
  (let [hv-seq (interpose "," (for [[th tv] tiles]
                                (format "h%02dv%02d" th tv)))]
    (format "*{%s}*" (apply str hv-seq))))

(defn s3-path [path]
  (str "s3n://AKIAJ56QWQ45GBJELGQA:6L7JV5+qJ9yXz1E30e3qmm4Yf7E1Xs4pVhuEL8LV@" path))

(defn -main
  "Example usage:

   hadoop jar forma-standalone.jar modisdata/MOD13A3 reddoutput/ [8 6] [10 12]"
  [input-path output-path & tiles]
  (let [tileseq (map read-string tiles)
        tilestring (str input-path (apply tiles->globstring tileseq))]
    (modis-chunker s/forma-subsets s/chunk-size
                   (s3-path input-path)
                   (s3-path output-path))))
