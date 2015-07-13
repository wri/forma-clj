(ns forma.playground
  "This namespace is a playground for playing in the playground with
  play things.  It mostly includes handy Thrift objects and Thrift
  memory taps for playing with at the REPL."
  (:require [forma.thrift :as thrift]))

;; MODIS tile chunk:
(def tile-chunk
  (thrift/DataChunk* "tile-chunk"
                     (thrift/ModisChunkLocation* "500" 8 0 100 24000)
                     [1 1 1]
                     "16"
                     :date "2001"))

;; MODIS pixel chunk:
(def pixel-chunk
  (thrift/DataChunk* "pixel-chunk" (thrift/ModisPixelLocation* "500" 1 2 3 4)
                     [1 1 1] "16" :date "2001"))

;; FireValue
(def fire-pixel-value
  (thrift/DataChunk* "fire-pixel"
                     (thrift/ModisPixelLocation* "500" 1 2 3 4)
                     (thrift/FireValue* 0 0 0 0)
                     "16"
                     :date "2001"))

;; Cascalog memory tap for tile chunks:
(def tile-chunk-tap [[tile-chunk]])

(def ts (let [ts (repeat 10 100)]
          (thrift/TimeSeries* 0 (dec (count ts)) ts)))

;; FireValue

(def fv (thrift/FireValue* 1 1 1 1))

;; Fire TS

(def fs (thrift/TimeSeries* 360 [fv fv]))

;; est-map

(def est-map {:est-start "1970-01-01"
              :est-end "2012-01-01"
              :t-res "16"})
