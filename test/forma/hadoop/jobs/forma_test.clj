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
   :min-coast-dist 3})

(def t-res "16")
(def s-res "500")
(def loc-vec [s-res 28 8 0 0])
(def pixel-loc (apply thrift/ModisPixelLocation* loc-vec))
(def vcf-src    [(conj loc-vec 26)]) 
(def hansen-src [(conj loc-vec 100)])
(def ecoid-src  [(conj loc-vec 15000)])
(def gadm-src   [(conj loc-vec 40132)])
(def border-src [(conj loc-vec 4)])


(comment
  pail-src (to-pail "/tmp/pail-path" sample-hansen-src)
  src (split-chunk-tap pail-src ["hansen"]))

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
  => (produces [(conj loc-vec 26 40132 15000 100 4)]))

(fact
  "Checks that `within-tileset?` correctly handles data inside and outside the given tile-set"
  (let [tile-set #{[28 8]}]
    (within-tileset? tile-set 28 8)
    (within-tileset? tile-set 29 8)))

(fact
  "Checks that query correctly screens out pixels not in tile-set"
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
  (let [forma-val (thrift/FormaValue* (thrift/FireValue* 0 0 0 0) 1.0 2.0 3.0 4.0)
        neighbor-val (thrift/NeighborValue* (thrift/FireValue* 1 0 0 1)
                                            1 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0)
        dynamic-src [["500" 827 28 8 0 0 forma-val neighbor-val]
                     ["500" 828 28 8 0 0 forma-val neighbor-val]]
        static-src [["500" 28 8 0 0 26 40132 15000 100 4]]]
    (beta-data-prep test-map dynamic-src static-src) => (produces [["500" 827 28 8 0 0 forma-val neighbor-val 15000 100]])))

(future-fact
 "figure out why the test above fails.")
