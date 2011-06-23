(ns forma.reproject-test
  (:use [forma.reproject] :reload)
  (:use midje.sweet
        [forma.matrix.utils :only (rowcol->idx)]))

(tabular
 (fact "line-torus test."
   (line-torus ?dir ?range ?corner ?val) ?arrow ?result)
 ?dir ?range ?corner ?val ?arrow ?result
 + 180  45  46 => 1
 + 180 -90 -88 => 2
 + 180 -88 -90 => 178
 - 180  45  46 => 179
 + 10   -2  54 => 6)

(def ascii-map {:step 0.5
                :corner [0 -90]
                :travel [+ +]})

(let [step 0.5
      [width] (dimensions-for-step step)]
  (facts
    "This is the only one I have so far... but we need to verify at
least one of these indices."
    (wgs84-indexer "1000" ascii-map 8 6 12 12) => [239 489]
    (rowcol->idx width 239 489) => 172569))

(fact (dimensions-for-step 0.5) => [720 360])
(fact (latlon->rowcol 0.5 + + -90 0 -42.1 0) => [95 0])
