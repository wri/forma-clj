(ns forma.playground
  "This namespace is a playground for playing in the playground with
  play things.  It includes handy Thrift objects and Thrift
  memory taps for playing with at the REPL, as well as dynamic charts."
  (:require [forma.thrift :as thrift]
            [incanter.charts :as charts]
            [incanter.core :as i]
            [incanter.stats :as stats]))

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

(def ts (thrift/TimeSeries* 0 1 (repeat 10 100)))

;; FireValue

(def fv (thrift/FireValue* 1 1 1 1))

;; Fire TS

(def fs (thrift/TimeSeries* 360 [fv fv]))

;; est-map

(def est-map {:est-start "1970-01-01"
              :est-end "2012-01-01"
              :t-res "16"})

(let [x (range -3 3 0.1)]
  (def pdf-chart (charts/xy-plot))
  (i/view pdf-chart) 
  (charts/sliders [mean (range -3 3 0.1) 
                   stdev (range 0.1 10 0.1)]
                  (i/set-data pdf-chart [x (stats/pdf-normal x :mean mean :sd stdev)])))

