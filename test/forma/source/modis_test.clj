(ns forma.source.modis-test
  (:use [forma.source.modis] :reload)
  (:use midje.sweet))

(facts
  "Lat lon conversions."
  (latlon->modis "1000" 29.89583 -115.2290) => [8 6 12 12]
  (latlon->modis "1000" 42.4 -115.1) => [9 4 600 912]
  (let [[lat lon] (modis->latlon "1000" 8 6 12 12)]
    lat => (roughly 29.90)
    lon => (roughly -115.23 0.01))
  (forma.source.modis/latlon->modis "1000" 29.90,-115.23) => [8 6 12 12])

(fact (tile-position "1000" 24000 2 100) => [100 40])
