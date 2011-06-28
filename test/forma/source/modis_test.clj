(ns forma.source.modis-test
  (:use [forma.source.modis] :reload)
  (:use midje.sweet))

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
  (tile-position "123" 24000 2 1) => (throws NullPointerException))

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
