(ns forma.reproject-test
  (:use [forma.reproject] :reload)
  (:use midje.sweet
        [forma.matrix.utils :only (rowcol->idx)]))

(facts "Bucketing values, as in `bucket`."
  (bucket 0.4 1.3) => 3
  (bucket 0.9 1.3) => 1)

(facts "travel test. As described in the function, travel ends up at
the centroid of the square reached. The value is calculated by walking
from the starting position in step-sized steps in the supplied
direction, then half-stepping to the centroid."
  (travel 0.5 + 0 2) => 1.25
  (travel 0.1 - 45 3) => (roughly 44.65))

(fact "dims for step test."
  (dimensions-for-step 0.5) => [720 360]
  (dimensions-for-step 1) => [360 180])

(tabular
 (fact "line-torus test."
   (line-torus ?dir ?range ?corner ?val) ?arrow ?result)
 ?dir ?range ?corner ?val ?arrow ?result
 + 180  45  46 => 1
 + 180 -90 -88 => 2
 + 180 -88 -90 => 178
 - 180  45  46 => 179
 + 10   -2  54 => 6)

(fact (latlon->rowcol 0.5 + + -90 0 -42.1 0) => [95 0])

;;TODO:
;;
;; test for rowcol->latlon, modis-indexer, 
;; 
;; THIS IS BUSTED! The modis indexer is freaking failing.
;.;. When someone asks you if you're a god, you say 'YES'! -- Zeddemore
(facts
  "Tests for the various indexers."
  (let [ascii-map {:step 0.5
                   :corner [0 -90]
                   :travel [+ +]}]
    (wgs84-indexer "1000" ascii-map 8 6 12 12) => [239 489]
    ;; (modis-indexer "1000" ascii-map 239 489)
    ))
