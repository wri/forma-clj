(ns forma.source.static-test
  (:use [forma.source.static] :reload)
  (:use cascalog.api
        midje.sweet))

;; Create a sample cascalog source comprised of MODIS resolution,
;; horizontal tile, vertical tile, row (line) and column (sample).

(def temp-mod-source
  (memory-source-tap [["1000" 28 7 208 2]
                      ["1000" 28 7 208 3]
                      ["1000" 28 7 208 4]
                      ["1000" 28 7 208 5]
                      ["1000" 28 7 208 6]]))

(def ascii-info {:corner [-180 83.8]
                 :travel [+ -]
                 :step 0.1
                 :nodata -9999})
