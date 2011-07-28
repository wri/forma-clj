(ns juke.reproject-test
  (:use [juke.reproject] :reload)
  (:use cascalog.api
        midje.sweet
        [juke.matrix.utils :only (rowcol->idx)]))

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
  (wgs84-resolution "1000") => (roughly 0.00833))

(facts "valid-modis? testing."
  "First version takes a sequence of tiles."
  (valid-modis? [[8 6]]) => truthy

  "Then, just modis h and v coords."
  (valid-modis? 8 6) => truthy
  (valid-modis? -1 -1) => falsey

  "Final version takes a full set of coords and a resolution."
  (valid-modis? "1000" 2 4 12 12) => falsey
  (valid-modis? "1000" 8 6 12 12) => truthy

  "Test for acceptable pixel range across resolutions."
  (valid-modis? "1000" 13 13 1200 1394) => falsey
  (valid-modis? "500" 13 13 1200 1394) => truthy)

(tabular
 (facts "hv and tilestring conversions. Make sure that invalid tiles
 aren't allowed through."
   (hv->tilestring ?mod-h ?mod-v) => ?tilestring)
 ?mod-h ?mod-v ?tilestring
 13     12     "013012"
 10      1     (throws AssertionError)
 -1     -1     (throws AssertionError))

(tabular
 (fact "tilestring->hv tests. Again, checking for malformed inputs."
   (tilestring->hv ?tilestring) => ?result)
 ?tilestring ?result
 "013012"    [13 12]
 "010001"    (throws AssertionError))

(facts "tile-position tests."
  (tile-position "1000" 24000 2 1231) => [31 41]
  (tile-position "500" 24000 2 1231) => [1231 20]
  (tile-position "500" 24000 -2 1) => (throws AssertionError)
  (tile-position "123" 24000 2 1) => (throws NullPointerException)

  "Fully defined cell dimension version."
  (tile-position 10 10 0 0 1) => [1 0]
  (tile-position 10 10 0 0 99) => [9 9]
  (tile-position 10 10 2 1 0) => [20 10]

  "can't have non-positive width or height for window."
  (tile-position -1 1 2 1 10) => (throws AssertionError))

(fact "testing the sinusoidal-xy conversions. These coords are
somewhere in Indonesia."
  (let [[deg-lat deg-lon] [4.2 97.3]
        [rad-lat rad-lon] (map #(Math/toRadians %) [deg-lat deg-lon])
        [rad-x rad-y] (latlon-rad->sinu-xy rad-lat rad-lon)
        [deg-x deg-y] (latlon->sinu-xy deg-lat deg-lon)]
    "Both radian and degrees match (tautological!)"
    rad-x => (roughly 1.079E7)
    deg-x => (roughly 1.079E7)

    "Same with y coords."
    rad-y => (roughly 466994.826)
    deg-y => (roughly 466994.826)))

(facts "exception throwing on sinu-xy conversions. lat and lon must be
well formed!"
  (latlon->sinu-xy 92 90) => (throws AssertionError)
  (latlon->sinu-xy 90 -181) => (throws AssertionError))

(tabular
 (facts "pixel-length tests."
   (pixel-length ?res) => ?result)
 ?res   ?result
 "1000" (roughly 926.625)
 "500"  (roughly 463.312)
 "250"  (roughly 231.66)
 "100"  (throws NullPointerException))

(facts
  "Lat lon conversions."
  (latlon->modis "1000" 29.89583 -115.2290) => [8 6 12 12]
  (latlon->modis "1000" 42.4 -115.1)        => [9 4 600 912]
  (let [[lat lon] (modis->latlon "1000" 8 6 12 12)]
    lat => (roughly 29.90)
    lon => (roughly -115.23 0.01)))

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
   (let [ascii-map {:step (wgs84-resolution ?res)
                    :corner [0 -90]
                    :travel [+ +]}]
     (apply modis-indexer ?res ascii-map ?wgs-coords) => ?mod-coords
     (apply wgs84-indexer ?res ascii-map ?mod-coords) => ?wgs-coords))
 ?res   ?mod-coords      ?wgs-coords
 "1000" [8 6 12 12]      [14387 29372]
 "1000" [29 10 100 1120] [8479 14095]
 "500" [25 7 2399 2399]  [24000 19495]
 "250" [29 10 3432 1222] [37177 57608])
