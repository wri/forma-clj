(ns forma.reproject-test
  (:use [forma.reproject] :reload)
  (:use midje.sweet))

(let [rain-indexer (wgs84-indexer "1000" 0.5 + + -90 0)]
  (fact
   "This is the only one I have so far... but we need to verify at
least one of these indices."
   (rain-indexer 8 6 12 12) => 172569))

(fact (dimensions-for-step 0.5) => [720 360])
(fact (fit-to-grid 0.5 + + -90 0 -42.1 0) => [95 0])
