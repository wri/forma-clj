(ns forma.hadoop.jobs.forma-test
  "Check that each step yields an intermediate result for the local
  data set, which runs from the end of the preprocessing through the
  final probablities.

  We are sourcing from a selection of about 2,300 pixels in Indonesia,
  of which 1,359 are hit with probabilities above 50% by April 2012.

  Only certain functions are tested on the small-sample data set,
  since even with the small selection, some of the functions take a
  few minutes to finish -- let alone check certain values. TODO: for
  these functions, come up with a few tests on an _even smaller_
  number of pixels."
  (:use forma.hadoop.jobs.forma :reload)
  (:use cascalog.api
        [midje sweet cascalog]
        [clojure.string :only (join)]
        [forma.hadoop.pail :only (to-pail split-chunk-tap)])
  (:require [forma.testing :as t]
            [forma.thrift :as thrift]))

(def test-map
  "Define estimation map for testing based on 500m-16day resolution.
  This is the estimation map that generated the small-sample test
  data."
  {:est-start "2005-12-31"
   :est-end "2012-04-22"
   :s-res "500"
   :t-res "16"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 30
   :window 10
   :ridge-const 1e-8
   :convergence-thresh 1e-6
   :max-iterations 500
   :min-coast-dist 3
   :nodata -9999.0})

(def t-res "16")
(def s-res "500")
(def loc-vec [s-res 28 8 0 0])
(def pixel-loc (apply thrift/ModisPixelLocation* loc-vec))
(def vcf-src    [(conj loc-vec 26)]) 
(def hansen-src [(conj loc-vec 100)])
(def ecoid-src  [(conj loc-vec 10101)])
(def gadm-src   [(conj loc-vec 40132)])
(def border-src [(conj loc-vec 4)])

(def sample-ts-dc-src
  (let [ts (thrift/TimeSeries* 693 [1 2 3])]
    [["pailpath" (thrift/DataChunk* "ndvi" pixel-loc ts "16")]
     ["pailpath" (thrift/DataChunk* "ndvi" (thrift/ModisPixelLocation* "500" 28 9 0 0) ts "16")]]))

(def sample-hansen-dc
  (thrift/DataChunk* "hansen" pixel-loc 100 t-res))

(defn sample-fire-series
  [start-idx length]
  (thrift/TimeSeries* start-idx (vec (repeat length (thrift/FireValue* 0 0 0 0)))))

(def sample-hansen-src
  (let [bad-loc (thrift/ModisPixelLocation* s-res 29 8 0 0)]
    [[(thrift/DataChunk* "hansen" pixel-loc 100 t-res)]
     [(thrift/DataChunk* "hansen" bad-loc 100 t-res)]]))

(fact
  "Checks that consolidate-static correctly merges static datasets."
  (consolidate-static (:vcf-limit test-map)
                      vcf-src gadm-src hansen-src ecoid-src border-src)
  => (produces [(conj loc-vec 26 40132 10101 100 4)]))

(fact
  "Checks that `within-tileset?` correctly handles data inside and outside the given tile-set"
  (let [tile-set #{[28 8]}]
    (within-tileset? tile-set 28 8)
    (within-tileset? tile-set 29 8)))

(fact
  "Checks that `screen-by-tileset` correctly screens out pixels not in tile-set"
  (let [tile-set #{[28 8]}
        bad-h 29
        src [["fake-path" sample-hansen-dc]
             ["fake-path" (thrift/DataChunk* "hansen"
                                 (thrift/ModisPixelLocation* s-res bad-h 8 0 0)
                                 100
                                 t-res)]]]
    (screen-by-tileset src tile-set)) => (produces [["fake-path" sample-hansen-dc]]))

(fact
  "Checks that fire-tap is trimming the fire timeseries correctly.
   Note that 828 is the period corresponding to 2006-01-01.
   Also note that the timeseries is a running sum."
  (let [src [[(thrift/DataChunk* "fire" pixel-loc (sample-fire-series 825 5) "01")]]]
    (fire-tap {:est-start "2005-12-31"
               :est-end "2006-01-01"
               :t-res t-res} src)) => (produces [[s-res 28 8 0 0 (sample-fire-series 827 2)]]))

(fact
  "Check that `filter-query` properly screens out the pixel with VCF < 25, and keeps the one with VCF >= 25."
  (let [vcf-limit 25
        static-src [["500" 28 8 0 0 25 0 0 0 0]
                    ["500" 28 9 0 0 10 0 0 0 0]]
        ts-src sample-ts-dc-src]
    (filter-query static-src vcf-limit ts-src)) => (produces [["500" 28 8 0 0 693 [1 2 3]]]))

(fact
  "Check `training-3000s?`"
  (training-3000s? "16" 826 "2005-12-31" [1 2 3 4]) => false
  (training-3000s? "16" 826 "2005-12-31" [-3000 2 3 4]) => false
  (training-3000s? "16" 826 "2005-12-31" [-3000 -3000 3 4]) => true
  (training-3000s? "16" 826 "2005-12-31" [-3000 -3000 -3000 4]) => true)

(fact
  "Check that `dynamic-filter` properly truncates values where timeseries
   lengths don't line up, and drops pixel with ."
  (let [ndvi-src [["500" 28 8 0 0 826 [1 2 3]]
                  ["500" 28 8 0 1 826 [-3000 -3000 3]]
                  ["500" 28 8 0 2 826 [-3000 2 3]]]
        reli-src [["500" 28 8 0 0 826 [0 0 3]]
                  ["500" 28 8 0 1 826 [0 0 3]]
                  ["500" 28 8 0 2 826 [0 0 3]]]
        rain-src [["500" 28 8 0 0 826 [0.1 0.2]]
                  ["500" 28 8 0 1 826 [0.1 0.2]]
                  ["500" 28 8 0 2 826 [0.1 0.2]]]]
    (dynamic-filter test-map ndvi-src reli-src rain-src)) => (produces [["500" 28 8 0 0 826 [1 2] [0.1 0.2] [0 0]]
                                                                        ["500" 28 8 0 2 826 [-3000 2] [0.1 0.2] [0 0]]]))

(tabular
 (fact
   "Check that pixel telescoping works as expected, avoiding any
    potential one-off errors lurking in the period indices.

    Note that period 693 represents 2000-02-18, and 827 represents
    2005-12-19, the end of the training period. A timeseries from 693
    to 827 should be 135 elements long - hence the use of `(range
    135)`."
   (let [src [[(vec (range 284))]]
         
         est-map (assoc-in test-map [:est-end] ?date)]
     (<- [?tele-ndvi]
         (src ?ndvi)
         (telescope-ts est-map 693 ?ndvi :> ?tele-ndvi))) => (produces ?result))
 ?date        ?result
 "2005-12-19" [[(vec (range 135))]]
 "2006-01-01" [[(vec (range 135))]
               [(vec (range 136))]]
 "2006-01-17" [[(vec (range 135))]
               [(vec (range 136))]
               [(vec (range 137))]])

(fact
  "Check that `max-nested-vec` returns maximum no matter the level of nesting"
  (max-nested-vec [1 2 3]) => 3
  (max-nested-vec [[1 2 3]]) => 3
  (max-nested-vec [[[1 2 3]]]) => 3)

(tabular
 (fact
   "Check `series-end`"
   (let [src [[[1 2 3]]]]
     (<- [?end]
         (src ?series)
         (series-end ?series ?start-idx :> ?end))) => (produces ?res))
?start-idx ?res
 2            [[4]]
 0            [[2]]
 50           [[52]])

(fact
  "Check `unnest-series`"
  (unnest-series [[[1 2 3]]]) => [1 2 3]
  (unnest-series [1 2 3]) => [1 2 3])

(fact
  "Check `unnest-all`"
  (unnest-all [[1 2 3]] [[4 5 6]] [7 8 9]) => [[1 2 3] [4 5 6] [7 8 9]])

(fact
  "Check that dynamic-clean actually replaces nodata value."
  (let [est-map {:est-start "2005-12-31"
                 :est-end "2006-02-03"
                 :t-res "16"
                 :nodata -9999}
        ts [["500" 28 8 0 0 826 [1 -9999 2 3 4] [1 1 1 1 1] [2 2 2 2 2]]]
        out-ts [["500" 28 8 0 0 826 [1 1 2 3 4] [1 1 1 1 1]]]]
    (dynamic-clean est-map ts) => (produces-some out-ts)))

(def good-val
  (thrift/FormaValue* (thrift/FireValue* 1. 1. 1. 1.) 1. 1. 1. 1.))

(def bad-val
  (thrift/FormaValue* (thrift/FireValue* 1. 1. 1. 1.) -9999. 1. 1. 1.))

(def static-src
  [["500" 28 8 0 0 25 0 1 100 20]   ;; all good
   ["500" 28 8 0 1 25 0 1 100 0]    ;; drop b/c of coast-dist
   ["500" 28 8 0 2 25 0 1 100 20]   ;; dyn. has nodata in forma-val
   ["500" 28 8 0 3 25 0 1 100 20]]) ;; dyn. has nodata in neighbor-val

(def dynamic-src
  [["500" 827 28 8 0 0 good-val good-val]  ;; ok
   ["500" 828 28 8 0 0 good-val good-val]  ;; not training
   ["500" 827 28 8 0 1 good-val good-val]  ;; bad static
   ["500" 827 28 8 0 2 bad-val good-val]   ;; forma-val has nodata
   ["500" 827 28 8 0 3 good-val bad-val]]) ;; neighbor-val has nodata

(fact
  "Check `beta-data-prep`.

   This query only checks that the correct pixels are returned. This
   is because midje doesn't seem to be able to do comparisons between
   thrift objects coming out of a Cascalog query - the `paramBreak`
   field appears to be missing from the thrift objects, but it is
   actually there."
  (let [prepped-src (beta-data-prep test-map dynamic-src static-src)
        result [["500" 827 28 8 0 0]]]
    (<- [?s-res ?pd ?mod-h ?mod-v ?s ?l]
     (prepped-src ?s-res ?pd ?mod-h ?mod-v ?s ?l _ _ _ _)) => (produces result)))
