(ns forma.source.modis-test
  (:use [forma.source.modis] :reload)
  (:use midje.sweet))

(tabular
 (fact "chunk-dims test. Currently, chunk-size has to be a
 whole-number multiple of the width of the MODIS tile. We also can't
 have negative or 0 chunk sizes."
   (chunk-dims ?resolution ?chunk-size) => ?result)
 ?resolution ?chunk-size ?result
 "1000"      24000       [1200 20]
 "1000"      23978       (throws AssertionError)
 "1000"      0           (throws AssertionError)
 "1000"      -2          (throws AssertionError)
 "1000"      978         (throws AssertionError)
 "500"       24000       [2400 10]
 "250"       24000       [4800 5])

(fact "wgs84-resolution test."
  )

(facts
  "Lat lon conversions."
  (latlon->modis "1000" 29.89583 -115.2290) => [8 6 12 12]
  (latlon->modis "1000" 42.4 -115.1) => [9 4 600 912]
  (let [[lat lon] (modis->latlon "1000" 8 6 12 12)]
    lat => (roughly 29.90)
    lon => (roughly -115.23 0.01)))

(fact (tile-position "1000" 24000 2 100) => [100 40])
