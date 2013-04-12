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
            [forma.schema :as schema])
  (:import [forma.schema DataChunk]))

(def test-map
  "Define estimation map for testing based on 500m-16day resolution.
  This is the estimation map that generated the small-sample test
  data."
  {:est-start "2005-12-31" ;; period index 827
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
  [start-idx length]
  (thrift/TimeSeries* start-idx (vec (repeat length (thrift/FireValue* 0 0 0 0)))))

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
  (let [src [[(thrift/DataChunk* "fire" pixel-loc (sample-fire-series 825 5) "01")]]
        est-start "2005-12-31"
        est-end "2006-01-01"]
    (fire-tap est-start est-end t-res src)) => (produces [[s-res 28 8 0 0 (sample-fire-series 827 2)]]))

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
  "Test `telescoping-trends` defmapcatop - math is tested elsewhere."
  (let [src [["500" 28 8 0 0 693 (vec (range 300))
                    (vec (map #(/ % 100.) (range 1 301)))]]]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?end-l ?short-l ?long-l ?t-stat-l ?break-l]
        (src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?ndvi ?precl)
        (telescoping-trends test-map 693 ?ndvi ?precl :> ?end-idx ?short ?long ?t-stat ?break)
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
  => (produces [["500" 28 8 0 0 693 973
                 0.9999999999999959 0.6468022465705872
                 6.0531753194685895E-6 136.1034297586291]]))

(fact "Test `trends-to-datachunks`."
  (let [start 827
        end 828
        ts [1. 2.]
        t-res (:t-res test-map)
        pedigree 1
        trends-src [["500" 28 8 0 0 start end ts ts ts ts]]
        dc (first (ffirst (??- (trends->datachunks test-map trends-src pedigree))))]
    (type dc) => DataChunk
    (.getDataset dc) => "trends"))

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
 (fact "Test `merge-trends`"
  (let [series [[1 ?first-series [1 2] [3 4] [5 6] [7 8]]
                [1 ?second-series [11 12] [13 14] [15 16] [17 18]]]]
    (merge-trends "16" -9999.0 series :consecutive ?consec))
  => ?result)
 ?consec ?first-series ?second-series ?result
 true :2005-12-19 :2006-01-17 [[827 [1 2 11 12] [3 4 13 14] [5 6 15 16] [7 8 17 18]]]
 false :2005-12-19 :2006-01-17 [[827 [1 2 11 12] [3 4 13 14] [5 6 15 16] [7 8 17 18]]]
 true :2005-12-19 :2006-02-02 (throws AssertionError)
 false :2005-12-19 :2006-02-02 [[827 [1 2 -9999.0 11 12] [3 4 -9999.0 13 14] [5 6 -9999.0 15 16] [7 8 -9999.0 17 18]]])

(future-fact "Test `merge-trends-wrapper`")
(future-fact "Test `forma-values->series`")
(future-fact "Test `trends-datachunks->series`")

(fact
  "Check forma-tap. This test got crazy because it seems that comparing
   thrift objects to one another doesn't work for checking a result."
  (let [dynamic-src [["500" 28 8 0 0 827 [827 828] [1. 2.] [3. 4.] [5. 6.] [7. 8.]]
                     ["500" 28 8 0 1 827 [827 828] [1. 2.] [3. 4.] [5. 6.] [7. 8.]]]
        fire-src [["500" 28 8 0 0 (sample-fire-series 827 2)]]
        first-period [1. 3. 5. 7.]
        second-period [1. 4. 6. 8.]
        empty-fire (thrift/FireValue* 0 0 0 0)
        forma-src (forma-tap test-map dynamic-src fire-src)]
    (<- [?s-res ?period ?mh ?mv ?s ?l ?fire-vec ?trends-stats]
        (forma-src ?s-res ?period ?mh ?mv ?s ?l ?forma-val)
        (thrift/unpack* ?forma-val :> ?forma-vec)
        (u/rest* ?forma-vec :> ?trends-stats)
        (first ?forma-vec :> ?fire-val)
        (thrift/unpack* ?fire-val :> ?fire-vec))
    => (produces [["500" 827 28 8 0 0 [0 0 0 0] first-period]
                  ["500" 828 28 8 0 0 [0 0 0 0] second-period]
                  ["500" 827 28 8 0 1 [0 0 0 0] first-period]
                  ["500" 828 28 8 0 1 [0 0 0 0] second-period]])))

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
                 7.0 12.0 8.0]]
               ["500" 827 28 8 1 0 [10 0 0 10]
                [3 6.333333333333333 1.0 7.333333333333333 2.0 8.333333333333334
                 3.0 9.333333333333334 4.0]]
               ["500" 827 28 8 0 1 [0 0 0 0]
                [3 7.666666666666667 1.0 8.666666666666666 2.0 9.666666666666666
                 3.0 10.666666666666666 4.0]]
               ["500" 827 28 8 1 1 [10 0 0 10]
                [3 5.0 1.0 6.0 2.0 7.0
                 3.0 8.0 4.0]]))

(fact
  "Check `process-neighbors`"
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
  => (produces [ [0 [10 0 0 10]
                  [3 9.0 5.0 10.0 6.0 11.0
                   7.0 12.0 8.0]]
                 [1 [10 0 0 10]
                  [3 6.333333333333333 1.0 7.333333333333333 2.0 8.333333333333334
                   3.0 9.333333333333334 4.0]]
                 [4 [0 0 0 0]
                  [3 7.666666666666667 1.0 8.666666666666666 2.0 9.666666666666666
                   3.0 10.666666666666666 4.0]]
                 [5 [10 0 0 10]
                  [3 5.0 1.0 6.0 2.0 7.0
                   3.0 8.0 4.0]]]))

(fact
  "Check `beta-gen`"
  (let [static-src [["500" 28 8 0 0 0 100]
                    ["500" 28 8 0 1 0 100]
                    ["500" 28 8 1 0 1 100]
                    ["500" 28 8 1 1 1 100]]
        val-src (neighbor-query (assoc test-map :window-dims [4 4]) val-src)
        src (<- [?s-res ?pd ?modh ?modv ?s ?l ?f-val ?n-val ?eco ?hansen]
                (static-src ?s-res ?modh ?modv ?s ?l ?eco ?hansen)
                (val-src ?s-res ?pd ?modh ?modv ?s ?l ?f-val ?n-val))]
    (beta-gen test-map src))
  => (produces [["500" 0 [0.0461579756378782 0.2702105185383453 0.0 0.0
                          0.27021000610469403 0.15424237795011647
                          0.20040035431283554 0.24655959531908314
                          0.2927166905266345 0.1913702772241395 0.0 0.0
                          0.19137137571486193 0.37939550810758377
                          0.12270652470850073 0.42555344181381133
                          0.16886473505952238 0.4717113982458025
                          0.21502319891919813 0.5178704313235151
                          0.2611812750199875]]
                ["500" 1 [0.039240667914858106 0.0 0.0 0.0 0.0
                          0.16182677835169845 0.2010738403141789
                          0.2403191044886442 0.279566649855779
                          0.392469357622685 0.0 0.0 0.39246900048887245
                          0.3123627337378208 0.0392468727065963
                          0.3516091392518591 0.07849384810832613
                          0.3908570303909329 0.11774050517175123
                          0.43010312243974663 0.15698786428781983]]]))

(fact
  "Test `apply-betas`"
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

(fact
  "Check `consolidate-timeseries`"
  (let [nodata -9999
        src [[1 827 1 2 3]
             [1 829 2 3 4]]]
    (<- [?id ?per-ts ?f1-ts ?f2-ts ?f3-ts]
        (src ?id ?period ?f1 ?f2 ?f3)
        (consolidate-timeseries nodata ?period ?f1 ?f2 ?f3 :> ?per-ts ?f1-ts ?f2-ts ?f3-ts))
    => (produces [[1 [827 -9999 829] [1 -9999 2] [2 -9999 3] [3 -9999 4]]])))

(fact
  "Test `forma-estimate`"
  (let [beta-src [["500" 0 (vec (repeat 21 0.5))]
                ["500" 1 (vec (repeat 21 0.75))]]
      static-src [["500" 28 8 0 0 25 0 1 0 0]]
      forma-val (thrift/FormaValue* (thrift/FireValue* 0 0 0 0) 1. 2. 3. 4.)
      neighbor-val (thrift/NeighborValue* (thrift/FireValue* 1 0 0 1) 1 -1. 2. -15. -2. -2. 6. -12. 8.)
      dyn-src [["500" 827 28 8 0 0 forma-val neighbor-val]
               ["500" 829 28 8 0 0 forma-val neighbor-val]]]
    (forma-estimate test-map beta-src dyn-src static-src))
  => (produces [["500" 28 8 0 0 [0.0953494648991095 -9999.0 0.0953494648991095]]]))
