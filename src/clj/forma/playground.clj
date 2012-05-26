(ns forma.playground
  (:require [forma.thrift :as thrift]))

;; MODIS tile chunk:
(def tile-chunk
  (thrift/DataChunk* "tile-chunk" (thrift/ModisChunkLocation* "500" 8 0 100 24000)
                     [1 1 1] "16" "2001"))

;; MODIS pixel chunk:
(def pixel-chunk
  (thrift/DataChunk* "pixel-chunk" (thrift/ModisPixelLocation* "500" 1 2 3 4)
                     [1 1 1] "16" "2001"))

;; Cascalog memory tap for tile chunks: 
(def tile-chunk-tap [[tile-chunk]])

;; Cascalog memory tap for pixel chunks: 
(def pixel-chunk-tap [[pixel-chunk]])

(def ts (thrift/TimeSeries* 0 1 (repeat 10 100)))
