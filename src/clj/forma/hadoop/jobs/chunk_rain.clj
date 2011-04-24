(ns forma.hadoop.jobs.chunk-rain
  (:use cascalog.api
        [forma.hadoop.io :only (chunk-tap
                                wholefile-tap)])
  (:require [forma.source.rain :as r]
            [forma.static :as s])
  (:gen-class))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res ll-res chunk-size tile-seq in-dir out-dir]
  (let [source (wholefile-tap in-dir)]
    (?- (chunk-tap out-dir)
        (r/rain-chunks m-res ll-res chunk-size tile-seq source))))

(defn s3-path [path]
  (str "s3n://AKIAJ56QWQ45GBJELGQA:6L7JV5+qJ9yXz1E30e3qmm4Yf7E1Xs4pVhuEL8LV@" path))

;; TODO: -- add precondition to check for valid-tiles
(defn -main
  "TODO: Example usage."
  [input-path output-path & tiles]
  (rain-chunker "1000" 0.5 s/chunk-size (map read-string tiles)
                (s3-path input-path)
                (s3-path output-path)))
