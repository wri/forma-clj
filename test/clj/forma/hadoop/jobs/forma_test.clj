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
        [forma.hadoop.pail :only (to-pail split-chunk-tap)]
        [forma.hadoop.predicate :as p]
        [forma.ops.classify :only (beta-dict)])
  (:require [forma.testing :as t]
            [forma.thrift :as thrift]
            [forma.utils :as u]
            [forma.date-time :as date]
            [forma.schema :as schema])
  (:import [forma.schema DataChunk]))

(def test-map
  "Define estimation map for testing based on 500m-16day resolution.
  This is the estimation map that generated the small-sample test
  data."
  {:est-start "2005-12-19" ;; period index 827
   :est-end "2012-04-22" ;; period index 973
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
    [[(thrift/DataChunk* "ndvi" pixel-loc ts "16")]
     [(thrift/DataChunk* "ndvi" (thrift/ModisPixelLocation* "500" 28 9 0 0) ts "16")]]))

(def sample-hansen-dc
  (thrift/DataChunk* "hansen" pixel-loc 100 t-res))

(defn sample-fire-series
  "Create a sample fire time series."
  [start-idx length & defaults]
  (thrift/TimeSeries* start-idx
                      (vec (repeat length (if defaults
                                            (apply thrift/FireValue* defaults)
                                            (thrift/FireValue* 0 0 0 0))))))

(def sample-hansen-src
  (let [bad-loc (thrift/ModisPixelLocation* s-res 29 8 0 0)]
    [[(thrift/DataChunk* "hansen" pixel-loc 100 t-res)]
     [(thrift/DataChunk* "hansen" bad-loc 100 t-res)]]))

(fact
  "Test `static-tap`"
  (let [pixel-loc (thrift/ModisPixelLocation* "500" 28 8 0 0)
      dc-src [["pail-path" (thrift/DataChunk* "vcf" pixel-loc 25 "00")]]]
    (static-tap dc-src)) => (produces [["500" 28 8 0 0 25]]))

(fact
  "Checks that consolidate-static correctly merges static datasets."
  (consolidate-static (:vcf-limit test-map)
                      vcf-src gadm-src hansen-src ecoid-src border-src)
  => (produces [(conj loc-vec 26 40132 10101 100 4)]))

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
  (let [start-idx 827
        len 5
        src [[(thrift/DataChunk* "fire" pixel-loc (sample-fire-series start-idx len) "01")]]]
    (fire-tap src) => (produces [[s-res 28 8 0 0
                                  (sample-fire-series start-idx len)]])))

(fact
  "Test `adjust-precl`"
  (let [base-t-res "32"
        target-t-res "16"
        precl-src [["500" 28 8 0 0 360 [1.5 2.5 3.4 4.7]]]]
    (adjust-precl base-t-res target-t-res precl-src))
  => (produces [["500" 28 8 0 0 690 [2 2 3 3 3 4 5]]]))

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
  "Check that `dynamic-filter` properly truncates values where
   timeseries lengths don't line up, drops pixel with all -3000s in
   the training period, and replaces a nodata value with the value to
   the left of it."
  (let [nodata (:nodata test-map)
        ndvi-src [["500" 28 8 0 0 826 [1 2 3]]
                  ["500" 28 8 0 1 826 [-3000 -3000 3]]
                  ["500" 28 8 0 2 826 [-3000 2 3]]]
        rain-src [["500" 28 8 0 0 826 [0.1 0.2]]
                  ["500" 28 8 0 1 826 [0.1 0.2]]
                  ["500" 28 8 0 2 826 [0.1 0.2]]]]
    (dynamic-filter test-map ndvi-src rain-src))
  => (produces [["500" 28 8 0 0 826 [1 2] [0.1 0.2]]
                ["500" 28 8 0 2 826 [-3000 2] [0.1 0.2]]]))

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
 "Test `calculate-trends` mechanics - math is checked elsewhere."
  (let [{:keys [window long-block]} test-map]
    (calculate-trends window long-block 693 (range 300 600) (range 800 499 -1)))
  => (list 992 nil nil 201.353677896894))

(fact
  "Check that trends calculations in `telescoping-trends` are
   independently calculated with regard to time. That is, if we
   calculate trends separately for two series using partially
   overlapping est-start and est-end parameters, the calculated trends
   coefficients should be identical for the overlapping periods. This
   test basically ensures that there isn't some bug in the interaction
   between est-start, est-end and the telescoping nature the trends
   coefficients calculation.

   In this test, the trends coefficients in the output series overlap
   for the period corresponding to 2012-04-22. Because the trends are
   calculated independently for each period, the coefficients should
   be identical for that period."
  (let [start-idx 693
        ndvi (vec (repeatedly 300 (partial rand-int 10000)))
        rain (vec (repeatedly 300 (partial rand 10)))
        test-map1 (assoc test-map :est-start "2005-12-19" :est-end "2012-04-22")
        test-map2 (assoc test-map :est-start "2012-04-22" :est-end "2012-04-22")]
    (= (map last (telescoping-trends test-map1 start-idx ndvi rain))
       (map last (telescoping-trends test-map2 start-idx ndvi rain)))) => true)

(fact
  "Test `telescoping-trends-wrapper` defmapcatop - math is tested elsewhere."
  (let [src [["500" 28 8 0 0 693 (vec (range 300))
                    (vec (map #(/ % 100.) (range 1 301)))]]]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?end-l ?short-l ?long-l ?t-stat-l ?break-l]
        (src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl)
        (telescoping-trends-wrapper test-map 693 ?ndvi ?precl :> ?end-idx ?short ?long ?t-stat ?break)
        (last ?end-idx :> ?end-l)
        (last ?short :> ?short-l)
        (last ?long :> ?long-l)
        (last ?t-stat :> ?t-stat-l)
        (last ?break :> ?break-l)))
  => (produces [["500" 28 8 0 0 693 973
                 0.9999999999999959 0.6468022465705872
                 6.0531753194685895E-6 136.1034297586291]]))

(fact
  "Test `analyze-trends` query - math is tested elsewhere"
  (let [src [["500" 28 8 0 0 693 (vec (range 300))
                    (vec (map #(/ % 100.) (range 1 301)))]]
        trends-src (analyze-trends test-map src)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?end ?short-l ?long-l ?t-stat-l ?break-l]
        (trends-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?end ?short ?long ?t-stat ?break)
        (last ?short :> ?short-l)
        (last ?long :> ?long-l)
        (last ?t-stat :> ?t-stat-l)
        (last ?break :> ?break-l)))
  => (produces [["500" 28 8 0 0 827 973
                 0.9999999999999959 0.6468022465705872
                 6.0531753194685895E-6 136.1034297586291]]))

(fact "Test `trends-to-datachunks`."
  (let [start 827
        end 828
        ts [1. 2.]
        t-res (:t-res test-map)
        trends-src [["500" 28 8 0 0 start end ts ts ts ts]
                    ["500" 28 8 0 0 start end [nil nil] ts ts ts]
                    ["500" 28 8 0 0 start end [nil nil] [nil nil] [nil nil] [nil nil] ]]
        dcs (??- (trends->datachunks test-map trends-src))]
    (type (first (ffirst dcs))) => DataChunk
    (.getDataset (first (ffirst dcs))) => "trends"
    (count (first dcs)) => 3))

(tabular
 (fact "Test `merge-sorted`."
  (let [t-res (:t-res test-map)
        nodata (:nodata test-map)]
    (merge-sorted t-res nodata 2 ?tuples :consecutive ?consec)) => ?result)
 ?consec ?tuples                  ?result
 ;; ok
 true    [[1 :2005-12-19 [1 2] [3 4] [5 6] [7 8]]
          [1 :2006-01-17 [11 12] [13 14] [15 16] [17 18]]]  [1 2 11 12]

 ;; not consecutive
 true    [[1 :2005-12-19 [1 2] [3 4] [5 6] [7 8]]
          [1 :2006-02-02 [11 12] [13 14] [15 16] [17 18]]]  (throws AssertionError)

 ;; not consecutive, fills in with nodata
 false   [[1 :2005-12-19 [1 2] [3 4] [5 6] [7 8]]
          [1 :2006-02-17 [11 12] [13 14] [15 16] [17 18]]]  [1 2 (:nodata test-map) 11 12])

(tabular
 (fact "Test `merge-series`."
   (let [series [[1 :2005-12-19 [1 2] [3 4] [5 6] [7 8]]
                [1 ?2nd-start-key [11 12] [13 14] [15 16] [17 18]]]]
    (merge-series "16" -9999.0 series :consecutive ?consec))
  => ?result)
 ?consec ?2nd-start-key ?result
 true :2006-01-17 [[827 [1 2 11 12] [3 4 13 14] [5 6 15 16] [7 8 17 18]]]
 false :2006-01-17 [[827 [1 2 11 12] [3 4 13 14] [5 6 15 16] [7 8 17 18]]]
 true :2006-02-02 (throws AssertionError)
 false :2006-02-02 [[827 [1 2 -9999.0 11 12] [3 4 -9999.0 13 14]
                     [5 6 -9999.0 15 16] [7 8 -9999.0 17 18]]])

(fact "Test `merge-series-wrapper`."
  (let [t-res (:t-res test-map)
        nodata (:nodata test-map)
        src [[50 0 :2005-12-19 [1 2 3] [2 3 4] [3 4 5] [4 5 6]]
             [50 1 :2006-01-01 [11] [12] [13] [14]] ;; replaces 2nd element
             [50 1 :2006-01-17 [6] [7] [8] [9]]]] ;; replaces 3rd element
    (<- [?id ?start-final ?short-final ?long-final ?t-stat-final ?break-final]
        (src ?id ?created ?start-key ?short ?long ?t-stat ?break)
        (merge-series-wrapper [t-res nodata] ?created ?start-key ?short
                              ?long ?t-stat ?break :> ?start-final ?short-final
                              ?long-final ?t-stat-final ?break-final)))
  => (produces [[50 827 [1 11 6] [2 12 7] [3 13 8] [4 14 9]]]))

(fact "Test `array-val->series`."
  (let [fire-val (thrift/FireValue* 0 0 0 0)
        forma-vals [(thrift/FormaValue* fire-val 1. 2. 3. 4.)
                    (thrift/FormaValue* fire-val 2. 3. 4. 5.)]
        array-val (last (thrift/unpack (thrift/TimeSeries* 827 forma-vals)))]
    (array-val->series array-val)
    => (produces [[fire-val fire-val] [1. 2.] [2. 3.] [3. 4.] [4. 5.]]))
  (array-val->series 5) => (throws AssertionError))

(fact "Test `trends-datachunks->series`.

       Second element of `data` is overwritten with first element of
       `data2`. This is because the time series are converted into
       maps then merged left to right (per standard `merge` behavior),
       older to newer based on `pedigree`. Overlapping series
       therefore take the most recent value for a given date."
  (let [test-map (assoc test-map :est-start "2005-12-19" :est-end "2006-01-17")
        fire-val (thrift/FireValue* 0 0 0 0)
        forma-val (thrift/FormaValue* fire-val 1. 2. 3. 4.)
        forma-val2 (thrift/FormaValue* fire-val 11. 12. 13. 14.)
        data (thrift/TimeSeries* 827 [forma-val forma-val])
        data2 (thrift/TimeSeries* 828 [forma-val2 forma-val2]) ;; overlapping `data`
        src [["" (thrift/DataChunk* "trends" pixel-loc data "16" :pedigree 1)]
             ["" (thrift/DataChunk* "trends" pixel-loc data2 "16" :pedigree 10)]]]
    (trends-datachunks->series test-map src))
  => (produces [["500" 28 8 0 0 827 [1. 11. 11.] [2. 12. 12.]
                                    [3. 13. 13.] [4. 14. 14.]]]))

(fact
  "Check `forma-tap`. This test got crazy because it seems that
   comparing thrift objects to one another within Cascalog doesn't
   work for checking a result."
  (let [test-map (assoc test-map :est-start "2010-01-01" :est-end "2010-01-17")
        start-idx (date/datetime->period t-res "2005-12-19")
        val-2 200
        len 175
        p1 (vec (map float (range len)))
        p2 (vec (map float (range val-2 (+ val-2 len))))
        dynamic-src [["500" 28 8 0 0 start-idx p1 p1 p1 p1]
                     ["500" 28 8 0 1 start-idx p2 p2 p2 p2]]
        fire-src [["500" 28 8 0 0 (sample-fire-series 709 350 0 0 0 1)]]
        src (forma-tap test-map dynamic-src fire-src)

        est-start-idx (date/datetime->period "16" "2010-01-01")
        ridx1a (- est-start-idx start-idx)
        ridx1b (inc ridx1a)
        ridx2a (+ val-2 ridx1a)
        ridx2b (inc ridx2a)
        res1a ["500" 920 28 8 0 0 [0 0 0 1] [0. ridx1a ridx1a ridx1a]]
        res1b ["500" 921 28 8 0 0 [0 0 0 1] [0. ridx1b ridx1b ridx1b]]
        res2a ["500" 920 28 8 0 1 [0 0 0 0] [val-2 ridx2a ridx2a ridx2a]]
        res2b ["500" 921 28 8 0 1 [0 0 0 0] [val-2 ridx2b ridx2b ridx2b]]]
    (<- [?s-res ?period ?mod-h ?mod-v ?sample ?line ?fire-vec ?trends-stats]
        (src ?s-res ?period ?mod-h ?mod-v ?sample ?line ?forma-val)
        (thrift/unpack* ?forma-val :> ?forma-vec)
        (u/rest* ?forma-vec :> ?trends-stats)
        (first ?forma-vec :> ?fire-val)
        (thrift/unpack* ?fire-val :> ?fire-vec))))

(fact "Check `eco-and-super`."
  (let [src [[1 10101]]]
    (<- [?a ?ecoregion]
        (src ?a ?ecoid)
        (eco-and-super ?ecoid :> ?ecoregion))) => (produces [[1 10101]
                                                             [1 21]]))
(def good-val
  (thrift/FormaValue* (thrift/FireValue* 1. 1. 1. 1.) 1. 1. 1. 1.))

(def bad-val
  (thrift/FormaValue* (thrift/FireValue* 1. 1. 1. 1.) -9999. 1. 1. 1.))

(def static-src
  [["500" 28 8 0 0 25 0 10101 100 20]   ;; all good
   ["500" 28 8 0 1 25 0 10101 100 0]    ;; drop b/c of coast-dist
   ["500" 28 8 0 2 25 0 10101 100 20]   ;; dyn. has nodata in forma-val
   ["500" 28 8 0 3 25 0 10101 100 20]]) ;; dyn. has nodata in neighbor-val

(def dynamic-src
  [["500" 827 28 8 0 0 good-val good-val]  ;; ok
   ["500" 828 28 8 0 0 good-val good-val]  ;; not training
   ["500" 827 28 8 0 1 good-val good-val]  ;; bad static
   ["500" 827 28 8 0 2 bad-val good-val]   ;; forma-val has nodata
   ["500" 827 28 8 0 3 good-val bad-val]]) ;; neighbor-val has nodata

(fact "Check `beta-data-prep`.

   This query only checks that the correct pixels are returned. This
   is because midje doesn't seem to be able to do comparisons between
   thrift objects coming out of a Cascalog query - the `paramBreak`
   field appears to be missing from the thrift objects, but it is
   actually there."
  (let [prepped-src (beta-data-prep test-map dynamic-src static-src)
        result [["500" 827 28 8 0 0]]]
    (<- [?s-res ?pd ?mod-h ?mod-v ?s ?l]
        (prepped-src ?s-res ?pd ?mod-h ?mod-v ?s ?l _ _ _ _)) => (produces result)))

(fact "Check `beta-data-prep` using super-ecoregions."
  (let [prepped-src (beta-data-prep test-map dynamic-src static-src
                                    :super-ecoregions true)
        result [["500" 827 28 8 0 0 21]
                ["500" 827 28 8 0 0 10101]]]
    (<- [?s-res ?pd ?mod-h ?mod-v ?s ?l ?ecoregion]
        (prepped-src ?s-res ?pd ?mod-h ?mod-v ?s ?l _ _ ?ecoregion _)) => (produces result)))

(def val-src
  (let [forma-val-1 (thrift/FormaValue* (thrift/FireValue* 0 0 0 0) 1. 2. 3. 4.)
        forma-val-2 (thrift/FormaValue* (thrift/FireValue* 10 0 0 10) 5. 6. 7. 8.)
        forma-val-3 (thrift/FormaValue* (thrift/FireValue* 0 0 0 0) 9. 10. 11. 12.)
        forma-val-4 (thrift/FormaValue* (thrift/FireValue* 0 0 0 0) 13. 14. 15. 16.)]
    (let [src [(conj ["500" 827 28 8 0 0] forma-val-1)
               (conj ["500" 827 28 8 0 1] forma-val-2)
               (conj ["500" 827 28 8 1 0] forma-val-3)
               (conj ["500" 827 28 8 1 1] forma-val-4)]]
      (<- [?s-res ?period ?modh ?modv ?sample ?line ?forma-val]
          (src ?s-res ?period ?modh ?modv ?sample ?line ?forma-val)))))

(fact
  "Check `neighbor-query`. Note that the val-src MUST be a query for the
   predicate macro within `neighbor-query` to work properly.

   Aside from that, this is also a crazy test in part because of the
   issue comparing thrift objects with Cascalog mentioned in the test
   above."
  (let [output-src (neighbor-query (assoc test-map :window-dims [4 4]) val-src)]
    (<- [?s-res ?pd ?modh ?modv ?sample ?line ?unpacked-fire ?neighbor-sans-fire]
        (output-src ?s-res ?pd ?modh ?modv ?sample ?line ?forma-val ?neighbor-val)
        (thrift/unpack* ?neighbor-val :> ?unpacked-neighbor)
        (first ?unpacked-neighbor :> ?fire-val)
        (thrift/unpack* ?fire-val :> ?unpacked-fire)
        (u/rest* ?unpacked-neighbor :> ?neighbor-sans-fire)))
  => (produces ["500" 827 28 8 0 0 [10 0 0 10]
                [3 9.0 5.0 10.0 6.0 11.0
                 7.0 12.0 16.0]]
               ["500" 827 28 8 1 0 [10 0 0 10]
                [3 6.333333333333333 1.0 7.333333333333333 2.0 8.333333333333334
                 3.0 9.333333333333334 16.0]]
               ["500" 827 28 8 0 1 [0 0 0 0]
                [3 7.666666666666667 1.0 8.666666666666666 2.0 9.666666666666666
                 3.0 10.666666666666666 16.0]]
               ["500" 827 28 8 1 1 [10 0 0 10]
                [3 5.0 1.0 6.0 2.0 7.0
                 3.0 8.0 12.0]]))

(fact "Check `process-neighbors`."
  (let [window-dims [4 4]
        nodata -9999.0
        neighbors 1
        src (p/sparse-windower val-src
                               ["?sample" "?line"]
                               window-dims
                               "?forma-val"
                               nil)]
    (<- [?win-idx ?unpacked-fire ?neighbor-sans-fire]
        (src _ _ _ _ _ _ ?window)
        (process-neighbors [neighbors] ?window nodata :> ?win-idx ?val ?neighbor-val)
        (thrift/unpack* ?neighbor-val :> ?unpacked-neighbor)
        (first ?unpacked-neighbor :> ?fire-val)
        (thrift/unpack* ?fire-val :> ?unpacked-fire)
        (u/rest* ?unpacked-neighbor :> ?neighbor-sans-fire)))
  => (produces [[0 [10 0 0 10]
                 [3 9.0 5.0 10.0 6.0 11.0
                  7.0 12.0 16.0]]
                [1 [10 0 0 10]
                 [3 6.333333333333333 1.0 7.333333333333333 2.0 8.333333333333334
                  3.0 9.333333333333334 16.0]]
                [4 [0 0 0 0]
                 [3 7.666666666666667 1.0 8.666666666666666 2.0 9.666666666666666
                  3.0 10.666666666666666 16.0]]
                [5 [10 0 0 10]
                 [3 5.0 1.0 6.0 2.0 7.0
                  3.0 8.0 12.0]]]))

(fact "Check `beta-gen`."
  (let [static-src [["500" 28 8 0 0 0 100]
                    ["500" 28 8 0 1 0 100]
                    ["500" 28 8 1 0 1 100]
                    ["500" 28 8 1 1 1 100]]
        val-src (neighbor-query (assoc test-map :window-dims [4 4]) val-src)
        src (<- [?s-res ?pd ?modh ?modv ?s ?l ?f-val ?n-val ?eco ?hansen]
                (static-src ?s-res ?modh ?modv ?s ?l ?eco ?hansen)
                (val-src ?s-res ?pd ?modh ?modv ?s ?l ?f-val ?n-val))]
    (beta-gen test-map src))
  => (produces-some [["500" 0 [0.03481795333428514 0.19023298754763054 0.0 0.0
                               0.19023311168910914 0.11091077460158351
                               0.14572852773893888 0.18054622464680353
                               0.2153653479559746 0.15794244831003232 0.0 0.0
                               0.1579427630327753 0.2879936281344108
                               0.09799524526989288 0.32281259884613517
                               0.1328126514357654 0.35763006632605876
                               0.16763024221484457 0.3924455236311622
                               0.5570824078834775]]
                     ["500" 1 [0.02686081458291698 0.0 0.0 0.0 0.0
                               0.20223243625323004 0.22910267668530285
                               0.25597122617607954 0.2828394963131509
                               0.2686823909010932 0.0 0.0 0.26868357856921793
                               0.18335964668160154 0.026868260536130714
                               0.21022788972411788 0.053736554882291086
                               0.23709601366290886 0.080605079702997
                               0.263965715597543 0.46947304172682214]]]))

(fact "Test `apply-betas`."
  (let [forma-val (thrift/FormaValue* (thrift/FireValue* 0 0 0 0) 1. 2. 3. 4.)
        neighbor-val (thrift/NeighborValue* (thrift/FireValue* 1 0 0 1) 1 1. 2. 3. 4. 5. 6. 7. 8.)
        src [[1 0 forma-val neighbor-val]
             [2 1 forma-val neighbor-val]]
        betas (beta-dict [["500" 0 (vec (repeat 21 0.5))]
                          ["500" 1 (vec (repeat 21 0.75))]])]
    (<- [?id ?prob]
        (src ?id ?eco ?forma-val ?neighbor-val)
        (apply-betas [betas] ?eco ?forma-val ?neighbor-val :> ?prob)))
  => (produces [[1 0.9999999999771026]
                [2 1.0]]))

(fact "Check `consolidate-timeseries`."
  (let [nodata -9999
        src [[1 827 1 2 3]
             [1 829 2 3 4]]]
    (<- [?id ?per-ts ?f1-ts ?f2-ts ?f3-ts]
        (src ?id ?period ?f1 ?f2 ?f3)
        (consolidate-timeseries nodata ?period ?f1 ?f2 ?f3 :> ?per-ts ?f1-ts ?f2-ts ?f3-ts))
    => (produces [[1 [827 -9999 829] [1 -9999 2] [2 -9999 3] [3 -9999 4]]])))

(fact "Test `forma-estimate`."
  (let [beta-src [["500" 0 (vec (repeat 21 0.5))]
                  ["500" 21 (vec (repeat 21 0.55))]
                  ["500" 10101 (vec (repeat 21 0.75))]]
      static-src [["500" 28 8 0 0 25 0 10101 0 0]]
      forma-val (thrift/FormaValue* (thrift/FireValue* 0 0 0 0) 1. 2. 3. 4.)
      neighbor-val (thrift/NeighborValue* (thrift/FireValue* 1 0 0 1) 1 -1. 2. -15. -2. -2. 6. -12. 8.)
      dyn-src [["500" 827 28 8 0 0 forma-val neighbor-val]
               ["500" 829 28 8 0 0 forma-val neighbor-val]]]

    ;; no super-ecoregions
    (forma-estimate test-map beta-src dyn-src static-src)
    => (produces [["500" 28 8 0 0 827 [0.0953494648991095 -9999.0 0.0953494648991095]]])

    ;; use super-ecoregions
    (forma-estimate test-map beta-src dyn-src static-src :super-ecoregions true)
    => (produces [["500" 28 8 0 0 827 [0.16110894957658545 -9999.0 0.16110894957658545]]])))

(fact "Test `probs->datachunks."
  (let [pedigree 1
        start-idx 827
        series [0.1 0.2 0.3 0.4 0.5]
        prob-src [(into loc-vec [start-idx series])]]
    (probs->datachunks test-map prob-src :pedigree 1)
    => (produces [[(thrift/DataChunk* "forma" pixel-loc (thrift/TimeSeries* start-idx series) t-res :pedigree pedigree)]])))

(fact "Test `probs-datachunks->series."
  (let [start1 827
        start2 829
        series1 [0.1 0.2 0.3]
        series2 [0.10 0.11 0.12]
        dc1 (thrift/DataChunk* "forma" pixel-loc (thrift/TimeSeries* start1 series1) t-res :pedigree 1)
        dc2 (thrift/DataChunk* "forma" pixel-loc (thrift/TimeSeries* start2 series2) t-res :pedigree 2)
        dc-src [["" dc1] ["" dc2]]]
    (probs-datachunks->series test-map dc-src))
  => (produces [(into loc-vec [827 [0.1 0.2 0.1 0.11 0.12]])]))

(fact "Test `probs-gadm2`"
  (let [probs-src [["500" 28 8 0 0 827 [0.1 0.2 0.3]]]
        gadm2-src [["500" 28 8 0 0 1]]
        static-src [["500" 28 8 0 0 -1 -1 2 -1 -1]]]
    (probs-gadm2 probs-src gadm2-src static-src)) => (produces [["500" 28 8 0 0 827 [0.1 0.2 0.3] 1 2]]))
