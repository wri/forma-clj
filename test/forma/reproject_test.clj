(ns forma.reproject-test
  (:use [forma.reproject] :reload)
  (:use midje.sweet
        [forma.matrix.utils :only (rowcol->idx)]))

(let [step 0.5
      [width] (dimensions-for-step step)
      rain-indexer (wgs84-indexer "1000" step + + -90 0)]
  (facts
    "This is the only one I have so far... but we need to verify at
least one of these indices."
    (rain-indexer 8 6 12 12) => [239 489]
    (rowcol->idx width 239 489) => 172569))

(fact (dimensions-for-step 0.5) => [720 360])
(fact (latlon->rowcol 0.5 + + -90 0 -42.1 0) => [95 0])
