(ns forma.gfw.cdm-test
  "This namespace provides unit testing coverage for the forma.gfw.cdm namespace."
  (:use forma.gfw.cdm
        [midje sweet]))

(tabular
 (fact
   "Test latlon->tile function."
   (latlon->tile ?lat ?lon ?zoom) => ?tile)
 ?lat ?lon ?zoom ?tile
 41.850033 -87.65005229999997 16 [16811 24364 16]
 41.850033 -87.65005229999997 0 [0 0 0]
 "41.850033" "-87.65005229999997" (throws AssertionError))

(tabular
 (fact
   "Test read-latlon function."
   (read-latlon ?lat ?lon) => ?result)
 ?lat ?lon ?result
 "41.850033" "-87.65005229999997" [41.850033 -87.65005229999997]
 41.850033 -87.65005229999997 (throws AssertionError)
 "41.850033" -87.65005229999997 (throws AssertionError)
 41.850033 "-87.65005229999997" (throws AssertionError))

(tabular
 (fact
   "Test latlon-valid? function."
   (latlon-valid? ?lat ?lon) => ?result)
 ?lat ?lon ?result
 41.850033 -87.65005229999997 true
 90 180 true
 -90 -180 true
 90 -180 true
 -90 180 true
 0 0 true
 90.0 180.0 true
 -90.0 -180.0 true
 90.0 -180.0 true
 -90.0 180.0 true
 0 0 true
 0.0 0.0 true 
 "41.850033" "-87.65005229999997" false
 -91 0 false
 91 0 false
 -181 0 false
 181 0 false)



