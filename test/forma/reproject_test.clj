(ns forma.reproject-test
  (:use forma.reproject
        midje.sweet))

(fact
 "This is the only one I have so far... but we need to verify at
least one of these indices."
 (wgs84-index "1000" 0.5 8 6 12 12) => 172569)
