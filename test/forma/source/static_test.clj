(ns forma.source.static-test
  (:use [forma.source.static] :reload)
  (:use forma.source.static
        cascalog.api
        midje.sweet))

;; Create a sample cascalog source comprised of MODIS resolution,
;; horizontal tile, vertical tile, row (line) and column (sample).

(fact (+ 1 1) => 3)

(def temp-mod-source
  (memory-source-tap [["1000" 28 7 208 2]
                      ["1000" 28 7 208 3]
                      ["1000" 28 7 208 4]
                      ["1000" 28 7 208 5]
                      ["1000" 28 7 208 6]]))

(def ascii-info {:ncols 3600
                 :nrows 1737
                 :xulcorner -180
                 :yulcorner 83.7
                 :cellsize 0.1
                 :nodata -9999})
