(ns forma.reproject-test
  (:use [forma.reproject] :reload)
  (:use midje.sweet))

(fact
 "This is the only one I have so far... but we need to verify at
least one of these indices."
 (wgs84-index "1000" 0.5 8 6 12 12) => 172569)

(fact (dimensions-at-res 0.5) => [720 360])

(fact (fit-to-grid 0.5 -40 720 -42 0) => [4 0])
