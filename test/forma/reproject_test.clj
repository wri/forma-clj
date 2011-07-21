(ns forma.reproject-test
  (:use [forma.reproject] :reload)
  (:use cascalog.api
        midje.sweet
        [forma.matrix.utils :only (rowcol->idx)]))

(facts "Bucketing values, as in `bucket`."
  (bucket 0.4 1.3) => 3
  (bucket 0.9 1.3) => 1)

(facts "travel test. As described in the function, travel ends up at
the centroid of the square reached. The value is calculated by walking
from the starting position in step-sized steps in the supplied
direction, then half-stepping to the centroid."
  (travel 0.5 + 0 2) => 1.25
  (travel 0.1 - 45 3) => 44.65
  (travel 0.1 + 45 3) => 45.35)

(facts "Tests on constrain."
  (constrain 10 0 11) => 1
  
  "Specific tests on lat and lon constraints."
  (constrain-lat -92) => 88
  (constrain-lat 4.2) => (roughly 4.2)
  (constrain-lon -179) => -179
  (constrain-lon 422.2) => (roughly 62.2))

(fact "dims for step test."
  (dimensions-for-step 0) => (throws AssertionError)
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

(facts "Back and forth between latlon and rowcol."
  (latlon->rowcol 0.5 + + -90 0 -89.75 100.25) => [0 200]
  (latlon->rowcol 0.5 + + -90 0 -42.1 0) => [95 0]
  (rowcol->latlon 0.5 + + -90 0 95 0) => [-42.25 0.25]

  "These tests show that the system has the ability to wrap around."
  (rowcol->latlon 0.5 + + -90 0 365 200) => [-87.25 100.25]
  (rowcol->latlon 0.5 + + -90 0 365 475) => [-87.25 -122.25])

(fact "Row-Col validation test."
  (valid-rowcol? 0.5 -1 1) => falsey
  (valid-rowcol? 0 0 0) => (throws AssertionError))

(tabular
 (fact "Back and forth testing between indices for a series of MODIS
 resolutions. The ASCII resolution here is tagged to the MODIS
 resolution, helping to ensure a 1:1 ratio between ascii grid cells
 and MODIS pixels. (This assumption falls apart at the edges, where
 projections clash, of course."
   (let [ascii-map {:step (forma.source.modis/wgs84-resolution ?res)
                    :corner [0 -90]
                    :travel [+ +]}]
     (apply modis-indexer ?res ascii-map ?wgs-coords) => ?mod-coords
     (apply wgs84-indexer ?res ascii-map ?mod-coords) => ?wgs-coords))
 ?res   ?mod-coords      ?wgs-coords
 "1000" [8 6 12 12]      [14387 29372]
 "1000" [29 10 100 1120] [8479 14095]
 "500" [25 7 2399 2399]  [24000 19495]
 "250" [29 10 3432 1222] [37177 57608])
