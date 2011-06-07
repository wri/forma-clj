(ns forma.source.modis-test
  (:use [forma.source.modis] :reload)
  (:use midje.sweet
        clojure.test))

(facts
 "Lat lon conversions."
 (latlon->modis "1000" 29.89583 -115.2290) => [8 6 12 12]
 (latlon->modis "1000" 42.4 -115.1) => [9 4 600 912])

(fact (tile-position "1000" 24000 2 100) => [100 40])
