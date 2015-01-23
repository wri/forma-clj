(ns forma.gfw.cdm-test
  "This namespace provides unit testing coverage for the forma.gfw.cdm namespace."
  (:use forma.gfw.cdm
        cascalog.api
        [midje sweet cascalog]))

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

(tabular
 (fact
   "Test meters->maptile function"
   (meters->maptile ?x ?y ?z) => ?result)
 ?x ?y ?z ?result
 -1  1  1 [0 0 1]
 -1  1  4 [7 7 4]
  1  1  1 [1 0 1]
  1  1  4 [8 7 4]
 -1 -1  1 [0 1 1]
 -1 -1  4 [7 8 4]
  1 -1  1 [1 1 1]
  1 -1  4 [8 8 4])

(fact "Test `agg-sort-by-date`."
  (let [src [[25032 36353 16 71 2]
             [25032 36353 16 72 1]
             [25032 36353 16 70 1]
             [25032 36352N 16 72 1]]]
    (<- [?x ?y ?z ?periods-counts]
        (src ?x ?y ?z ?period ?count)
        (agg-sort-by-date ?period ?count :> ?periods-counts)))
  => (produces [[25032 36352N 16 [[72] [1]]]
                [25032 36353 16 [[70 71 72] [1 2 1]]]]))

(fact "Test `split-vecs`."
  (let [src [[1 2 3 [[71 72] [1 1]]]]]
    (<- [?x ?y ?z ?periods ?counts]
        (src ?x ?y ?z ?periods-counts)
        (split-vecs ?periods-counts :> ?periods ?counts)))
  => (produces [[1 2 3 [71 72] [1 1]]]))

(fact "Test `vec->arr-str`."
  (vec->arr-str [1 2.0 3]) => "\"{1,2.0,3}\"")

(fact "Test `arr-str->vec`."
  (arr-str->vec "\"{1,2.0,3}\"") => [1 2.0 3])

(fact "Test `zoom-out`."
  (zoom-out 50064) => 25032
  (zoom-out 21) => 10)

(fact "Test `gen-tiles`."
  (gen-tiles 50064 72706 17 14)
  => [[6258 9088N 14] [12516 18176N 15] [25032 36353 16] [50064 72706 17]])

(fact "Test `gen-tiles-mapcat`."
  (let [min-z 13
      src [[50064 72706 17]]]
    (<- [?x2 ?y2 ?z2]
      (src ?x ?y ?z)
      (gen-tiles-mapcat ?x ?y ?z min-z :> ?x2 ?y2 ?z2)))
  => (produces [[3129 4544N 13] [6258 9088N 14] [12516 18176N 15]
                [25032 36353 16] [50064 72706 17]]))

(fact "Test `gen-all-zooms`."
  (let [min-z 14
        src [[50064 72706 17 71]
             [50064 72707 17 72]
             [50064 72705 17 71]]] ;; last pixel
    (gen-all-zooms src min-z))
  => (produces [[6258 9088 14 71 2] ;; last pixel aggregated here
                [12516 18176 15 71 2] ;; and here
                [25032 36353 16 71 1]
                [50064 72706 17 71 1]
                [25032 36352 16 71 1] ;; last pixel appears here
                [50064 72705 17 71 1] ;; and here
                [6258 9088 14 72 1]
                [12516 18176 15 72 1]
                [25032 36353 16 72 1]
                [50064 72707 17 72 1]]))

(fact "Test agg-periods-counts`."
  (let [src [[6258 9088 14 71 2]
             [12516 18176 15 71 2]
             [25032 36353 16 71 1]
             [50064 72706 17 71 1]
             [25032 36352 16 71 1]
             [50064 72705 17 71 1]
             [6258 9088 14 72 1]
             [12516 18176 15 72 1]
             [25032 36353 16 72 1]
             [50064 72707 17 72 1]]]
    (agg-periods-counts src))
  => (produces [[6258 9088 14 "\"{71,72}\"" "\"{2,1}\""]
                [12516 18176 15 "\"{71,72}\"" "\"{2,1}\""]
                [25032 36352 16 "\"{71}\"" "\"{1}\""]
                [25032 36353 16 "\"{71,72}\"" "\"{1,1}\""]
                [50064 72705 17 "\"{71}\"" "\"{1}\""]
                [50064 72706 17 "\"{71}\"" "\"{1}\""]
                [50064 72707 17 "\"{72}\"" "\"{1}\""]]))
