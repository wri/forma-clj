(ns forma.hadoop.jobs.chunk-rain
  (:use forma.static
        cascalog.api
        [forma.hadoop.io :only (modis-seqfile all-files)])
  (:require [cascalog.ops :as c]
            [forma.source.rain :as r])
  (:gen-class))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res ll-res c-size tile-seq in-dir out-dir]
  (let [source (wholefile-tap in-dir)]
    (?- (modis-seqfile out-dir)
        (r/rain-chunks m-res ll-res c-size tile-seq source))))

(defn s3-path [path]
  (str "s3n://AKIAJ56QWQ45GBJELGQA:6L7JV5+qJ9yXz1E30e3qmm4Yf7E1Xs4pVhuEL8LV@" path))

;; TODO: -- add precondition to check for valid-tiles
(defn -main
  "TODO: Example usage."
  [input-path output-path & tiles]
  (rain-chunker "1000" 0.5 24000 (map read-string tiles)
                (s3-path input-path)
                (s3-path output-path)))
