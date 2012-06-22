(ns forma.hoptree-test
  (:use [forma.hoptree] :reload)
  (:use midje.sweet))

(facts "The number of rows and columns in the global MODIS grid; note
that the rectangle is twice as wide as it is high."
  (global-dims "500")  => '(43200 86400)
  (global-dims "1000") => '(21600 43200))

(facts "TileRowCol* generator should act on a vector of the MODIS tile
coordinates, as well as the set of individual elements as arguments"
  (global-index "500" (TileRowCol* [28 8 0 0])) => 1658947200
  (global-index "500" (TileRowCol* 28 8 0 0))   => 1658947200)

(facts "Given the index calculated in the previous test, the row and
column should be the same for the same pixel, identified by either its
tile coordinates or its global index."
  (global-rowcol "500" (TileRowCol* [28 8 0 0]))  => [19200 67200]
  (global-rowcol "500" (GlobalIndex* 1658947200)) => [19200 67200])

(facts "Translate a global coordinate to a window coordinate, based on
the window-map that defines the window; and then translate the window
coordinate to the global index.  Note that since the method
`global-index` may not operate on a WindowRowCol. instance, the
`window-map` argument is optional and appears at the end of the
argument list (which is a different pattern than the `window-rowcol`
and `window-index` methods. "
  (let [window-map {:topleft-rowcol [1 1] :width 4 :height 5}]
    (window-rowcol "500" window-map (GlobalRowCol* [5 1])) => [4 0]
    (global-index  "500" (GlobalRowCol* [5 1]))            => 432001
    (global-index  "500" (WindowRowCol* [4 0]) window-map) => 432001))

(facts "Collect the global indices for the 8 adjacent pixels -- as
well as the own-pixel index -- for a total of 9 indices"
  (let [idx-coll (neighbor-idx "500" (GlobalIndex* 432001))]
    
    (count idx-coll) => 9

    idx-coll => '(345600 345601 345602
                  432000 432001 432002
                  518400 518401 518402)

    (let [convert (fn [x] (global-rowcol "500" (GlobalIndex* x)))]
      (map convert idx-coll) => '([4 0] [4 1] [4 2]
                                  [5 0] [5 1] [5 2]
                                  [6 0] [6 1] [6 2]))))

