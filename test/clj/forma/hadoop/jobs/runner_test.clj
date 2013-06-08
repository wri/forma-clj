(ns forma.hadoop.jobs.runner-test
  (:use cascalog.api
        [midje sweet cascalog]
        forma.hadoop.jobs.runner
        [forma.source.tilesets :only (tile-set country-tiles)]
        [cascalog.checkpoint :only (workflow)]
        [forma.schema :only (series->forma-values)])
  (:require [forma.hadoop.jobs.forma :as forma]
            [forma.thrift :as thrift]
            [forma.date-time :as date]
            [cascalog.io :as io]
            [cascalog.ops :as c]
            [forma.hadoop.pail :as p]))

(def s-res "500")
(def t-res "16")
(def pix-loc [s-res 28 8 0 0])
(def modis-start "2000-02-18")
(def est-start "2005-12-19")
(def est-end "2006-01-01")
(def start-idx (date/datetime->period t-res modis-start))
(def end-idx (date/datetime->period t-res est-end))
(def series (vec (range (inc (- end-idx start-idx)))))
(def static-src [[s-res 28 8 0 0 50 -1 -1 -1 -1]])

(def ts-path (.getPath (io/temp-dir "ts-src")))
(def static-path (.getPath (io/temp-dir "static-src")))
(def output-path (.getPath (io/temp-dir "output-sink")))

(defn sample-fire-series
  "Create a sample fire time series. Duplicated from
   forma.hadoop.jobs.forma-test namespace."
  [start-idx length]
  (thrift/TimeSeries* start-idx (vec (repeat length (thrift/FireValue* 0 0 0 0)))))

(fact "Test `get-est-map`"
  (map (get-est-map "500" "16") [:est-start :est-end]) => ["2005-12-19" nil]
  (map (get-est-map "500" "16" :est-start "2006-01-01") [:est-start :est-end]) => ["2006-01-01" nil]
  (map (get-est-map "500" "16" :est-start "2006-01-01" :est-end "2007-01-01")
       [:est-start :est-end]) => ["2006-01-01" "2007-01-01"])

(future-fact
 "sample-fire-series is not duplicated from forma.hadoop.jobs.forma-test
  namespace")

(fact
  "Integration test of `TimeseriesFilter`. All queries and functions used
   are tested elsewhere."
  (let [loc (apply thrift/ModisPixelLocation* pix-loc)
        ts (thrift/TimeSeries* start-idx series)
        ts-src [[(thrift/DataChunk* "ndvi" loc ts s-res)]]
        _ (?- (hfs-seqfile ts-path :sinkmode :replace) ts-src)
        _  (?- (hfs-seqfile static-path :sinkmode :replace) static-src)
        _ (TimeseriesFilter s-res t-res ts-path static-path output-path)]
    (hfs-seqfile output-path)
    => (produces  [(vec (concat pix-loc [start-idx] [series]))])))

(fact
  "Integration test of `AdjustSeries` defmain. All queries and functions
   used are tested elsewhere."
  (let [ndvi-src [(concat pix-loc [start-idx] [series])]
        rain-src [(concat pix-loc [start-idx] [(vec (concat series [-1 -1]))])]
        ndvi-path (.getPath (io/temp-dir "ndvi-src"))
        rain-path (.getPath (io/temp-dir "rain-src"))
        output-path (.getPath (io/temp-dir "adjusted-src"))
        _ (?- (hfs-seqfile ndvi-path :sinkmode :replace) ndvi-src)
        _ (?- (hfs-seqfile rain-path :sinkmode :replace) rain-src)
        _ (AdjustSeries s-res t-res ndvi-path rain-path output-path)]
    (hfs-seqfile output-path)
    => (produces [(concat pix-loc [start-idx] [series] [series])])))

(facts
  "Integration test of `Trends` defmain. All queries and functions used are
   tested elsewhere"
  (let [est-end "2006-01-17"
        est-start "2006-01-17"
        src [[s-res 28 8 0 0 693 (conj series 140 139 140) (conj series 128 140 128)]]
        adjusted-path (.getPath (io/temp-dir "adjusted-src"))
        output-path (.getPath (io/temp-dir "trends-src"))
        _ (?- (hfs-seqfile adjusted-path :sinkmode :replace) src)]
    (Trends s-res t-res est-end adjusted-path output-path)
    (hfs-seqfile output-path)
    => (produces [[s-res 28 8 0 0 827 829
                   [0.9999999999999982 0.999999999999998 0.999999999999998]
                   [nil nil 1.4999999999989022]
                   [nil nil 8.131966242712587E10]
                   [67.93805204562999 72.56614847949598 17.048905109489304]]])

    (Trends s-res t-res est-end adjusted-path output-path est-start)
    (hfs-seqfile output-path)
    => (produces [[s-res 28 8 0 0 829 829
                   [0.999999999999998]
                   [1.4999999999989022]
                   [8.131966242712587E10]
                   [17.048905109489304]]])))

(fact "Integration test of `TrendsPail` defmain. All queries and
functions are tested elsewhere."
  (let [est-end "2006-01-17"
        pedigree 1
        src [[s-res 28 8 0 0 827 829 [1. 1. 1.] [2. 2. 2.] [3. 3. 3.] [4. 4. 4.]]]
        trends-path (.getPath (io/temp-dir "trends-src"))
        trends-pail-path (.getPath (io/temp-dir "trends-pail"))
        pail-tap (p/split-chunk-tap trends-pail-path ["trends" (format "%s-%s" s-res t-res)])
        _ (?- (hfs-seqfile trends-path :sinkmode :replace) src)]
    (TrendsPail s-res t-res est-end trends-path trends-pail-path pedigree)
    (<- [?type-n]
        (pail-tap _ ?dc)
        (type ?dc :> ?type)
        (c/limit [1] ?type :> ?type-n)))
  => (produces [[forma.schema.DataChunk]]))

(fact "Integration test of `MergeTrends` defmain. All queries and
  functions are tested elsewhere.

  Note that the expected behavior (given the supplied pedigree values)
  is that the second element of the first time series will be
  overwritten by the first element of the second time series as the
  time series are merged."
  (let [trends-pail-path (.getPath (io/temp-dir "trends-pail-merge"))
        merged-path (.getPath (io/temp-dir "trends-merged"))
        [forma-vals] (series->forma-values nil [1. 2.] [2. 3.] [3. 4.] [4. 5.])
        loc (apply thrift/ModisPixelLocation* pix-loc)
        ts-1 (thrift/TimeSeries* 827 forma-vals)
        ts-2 (thrift/TimeSeries* 828 forma-vals)
        pedigree1 1
        pedigree2 2
        src [[(thrift/DataChunk* "trends" loc ts-1 t-res :pedigree pedigree1)]
             [(thrift/DataChunk* "trends" loc ts-2 t-res :pedigree pedigree2)]]
        _ (p/to-pail trends-pail-path src)]
    (MergeTrends s-res t-res est-end trends-pail-path merged-path)
    (hfs-seqfile merged-path))
  => (produces [(into pix-loc [827 [1. 1. 2.] [2. 2. 3.] [3. 3. 4.] [4. 4. 5.]])]))

(fact
  "Integration test of `FormaTap` defmain. All queries and functions used
   are tested elsewhere."
  (let [est-start "2006-01-01"
        est-end "2006-01-01"
        fire-src [[s-res 28 8 0 0 (sample-fire-series 827 1)]]
        dynamic-src [[s-res 28 8 0 0 827 [1. 2. 3.] [3. 4. 5.] [5. 6. 7.] [7. 8. 9.]]]
        fire-path (.getPath (io/temp-dir "fire-src"))
        dynamic-path (.getPath (io/temp-dir "dynamic-src"))
        output-path (.getPath (io/temp-dir "forma-src"))
        _ (?- (hfs-seqfile dynamic-path :sinkmode :replace) dynamic-src)
        _ (?- (hfs-seqfile fire-path :sinkmode :replace) fire-src)
        _ (FormaTap s-res t-res est-start est-end fire-path dynamic-path output-path)]
    (let [src (hfs-seqfile output-path)]
      (<- [?s-res ?pd ?mod-h ?mod-v ?sample ?line ?fire-vec
           ?short ?long ?t-stat ?break]
          (src ?s-res ?pd ?mod-h ?mod-v ?sample ?line ?forma-val)
          (thrift/unpack ?forma-val :> ?fire-val ?short ?long ?t-stat ?break)
          (thrift/unpack* ?fire-val :> ?fire-vec)))
    => (produces [[s-res 827 28 8 0 0 [0 0 0 0] 2. 4. 6. 8.]])))

(fact
  "Integration test of `NeighborQuery` defmain. All queries and functions
   used are tested elsewhere."
  (let [forma-val (thrift/FormaValue* (thrift/FireValue* 1 1 1 1)
                                      1. 2. 3. 4.)
        val-src  [[s-res 827 28 8 0 0 (thrift/FormaValue*
                                       (thrift/FireValue* 1 1 1 1)
                                       1. 2. 3. 4.)]
                  [s-res 827 28 8 0 1 (thrift/FormaValue*
                                       (thrift/FireValue* 1 1 1 1)
                                       5. 6. 7. 8.)]]
        forma-val-path (.getPath (io/temp-dir "forma-val-src"))
        output-path (.getPath (io/temp-dir "neighbor-src"))
        _ (?- (hfs-seqfile forma-val-path :sinkmode :replace) val-src)]
    (NeighborQuery s-res t-res forma-val-path output-path)
    (let [src (hfs-seqfile output-path)]
      (<- [?s-res ?pd ?mod-h ?mod-v ?sample ?line ?a ?b ?c ?d ?e ?f ?g ?h ?i]
          (src ?s-res ?pd ?mod-h ?mod-v ?sample ?line ?forma-val ?neighbor-val)
          (thrift/unpack ?neighbor-val :> _ ?a ?b ?c ?d ?e ?f ?g ?h ?i))))
  => (produces [[s-res 827 28 8 0 0 1 5.0 5.0 6.0 6.0 7.0 7.0 8.0 8.0]
                [s-res 827 28 8 0 1 1 1.0 1.0 2.0 2.0 3.0 3.0 4.0 4.0]]))

(fact
  "Integration test of `BetaDataPrep` defmain. All queries and functions
   used are tested elsewhere."
  (let [forma-val (thrift/FormaValue* (thrift/FireValue* 1 1 1 1) 1. 2. 3. 4.)
        neighbor-val (thrift/NeighborValue* (thrift/FireValue* 1 1 1 1) 1 1. 2. 3. 4. 5. 6. 7. 8. 9.)
        neighbor-src [[s-res 827 28 8 0 0 forma-val neighbor-val]
                      [s-res 827 28 8 0 1 forma-val neighbor-val]
                      [s-res 900 28 8 0 0 forma-val neighbor-val]]
        static-src [[s-res 28 8 0 0 25 14000 1000 100 20]
                    [s-res 28 8 0 1 25 14000 1000 100 1]]
        neighbor-path (.getPath (io/temp-dir "neighbor-src"))
        static-path (.getPath (io/temp-dir "static-src"))
        output-path (.getPath (io/temp-dir "beta-data-src"))
        _ (?- (hfs-seqfile neighbor-path :sinkmode :replace) neighbor-src)
        _ (?- (hfs-seqfile static-path :sinkmode :replace) static-src)]
    (BetaDataPrep s-res t-res neighbor-path static-path output-path)
    (let [src (hfs-seqfile output-path)]
      (<- [?s-res ?pd ?mod-h ?mod-v ?sample ?line ?hansen ?ecoid]
          (src ?s-res ?pd ?mod-h ?mod-v ?sample ?line _ _ ?hansen ?ecoid)))
    => (produces [[s-res 827 28 8 0 0 1000 100]])))

(fact
  "Integration test of `GenBetas` defmain. All queries and functions
   used are tested elsewhere."
  (let [forma-val (thrift/FormaValue* (thrift/FireValue* 1 1 1 1)
                                      1. 2. 3. 4.)
        neighbor-val (thrift/NeighborValue* (thrift/FireValue* 1 1 1 1)
                                            1 1. 2. 3. 4. 5. 6. 7. 8. 9.)
        beta-data-src [[s-res 827 28 8 0 0 forma-val neighbor-val 1000 100]]
        beta-data-path (.getPath (io/temp-dir "beta-data-src"))
        output-path (.getPath (io/temp-dir "beta-src"))
        _ (?- (hfs-seqfile beta-data-path :sinkmode :replace) beta-data-src)]
    (GenBetas s-res t-res est-start beta-data-path output-path)
    (hfs-seqfile output-path))
  => (produces [["500" 1000 [0.11842551448466368 0.11842586443146805
                             0.1184259896040349 0.11842585137116168
                             0.11842584951322709 0.11842584885467246
                             0.23685185941367817 0.35527734116779586
                             0.4737032372673255 0.11842582139264606
                             0.11842582044881808 0.118425835272797
                             0.11842581441993594 0.1184258039325292
                             0.23685165512279985 0.35527741717744016
                             0.4737032025455936 0.5921290128620343
                             0.7105544981645566 0.8289803917212081
                             0.947406494802988]]]))

(fact
  "Integration test of `FormaEstimate` defmain. All queries and functions
   used are tested elsewhere."
  (let [forma-val (thrift/FormaValue* (thrift/FireValue* 1 1 1 1)
                                      1. 2. 3. 4.)
        neighbor-val (thrift/NeighborValue* (thrift/FireValue* 1 1 1 1)
                                            1 1. 2. 3. 4. 5. 6. 7. 8. 9.)
        beta-src [["500" 1000 [0.11842551448466368 0.11842586443146805
                               0.1184259896040349 0.11842585137116168
                               0.11842584951322709 0.11842584885467246
                               0.23685185941367817 0.35527734116779586
                               0.4737032372673255 0.11842582139264606
                               0.11842582044881808 0.118425835272797
                               0.11842581441993594 0.1184258039325292
                               0.23685165512279985 0.35527741717744016
                               0.4737032025455936 0.5921290128620343
                               0.7105544981645566 0.8289803917212081
                               0.947406494802988]]]
        neighbor-src [[s-res 827 28 8 0 0 forma-val neighbor-val]]
        static-src [[s-res 28 8 0 0 25 14000 1000 100 20]]
        neighbor-path (.getPath (io/temp-dir "to-estimate-src"))
        beta-path (.getPath (io/temp-dir "beta-src"))
        static-path (.getPath (io/temp-dir "static-src"))
        output-path (.getPath (io/temp-dir "estimated-src"))
        _ (?- (hfs-seqfile beta-path :sinkmode :replace) beta-src)
        _ (?- (hfs-seqfile neighbor-path :sinkmode :replace) neighbor-src)
        _ (?- (hfs-seqfile static-path :sinkmode :replace) static-src)]
    (EstimateForma s-res t-res beta-path neighbor-path static-path output-path)
    (hfs-seqfile output-path))
  => (produces [["500" 28 8 0 0 827 [0.9999999999996823]]]))

(fact "Integration test of `ProbsPail` defmain. All queries and
functions are tested elsewhere."
  (let [est-end "2006-01-17"
        pedigree 1
        name "forma"
        src [[s-res 28 8 0 0 827 [1. 1. 1.]]]
        probs-path (.getPath (io/temp-dir "probs-src"))
        probs-pail-path (.getPath (io/temp-dir "probs-pail"))
        pail-tap (p/split-chunk-tap probs-pail-path [name (format "%s-%s" s-res t-res)])
        _ (?- (hfs-seqfile probs-path :sinkmode :replace) src)]
    (ProbsPail s-res t-res est-end probs-path probs-pail-path pedigree)
    (<- [?type-n]
        (pail-tap _ ?dc)
        (type ?dc :> ?type)
        (c/limit [1] ?type :> ?type-n)))
  => (produces [[forma.schema.DataChunk]]))

(fact "Integration test of `MergeProbs` defmain. All queries and
  functions are tested elsewhere.

  Note that the expected behavior (given the supplied pedigree values)
  is that the second element of the first time series will be
  overwritten by the first element of the second time series as the
  time series are merged."
  (let [probs-pail-path (.getPath (io/temp-dir "probs-pail-merge"))
        merged-path (.getPath (io/temp-dir "probs-merged"))
        loc (apply thrift/ModisPixelLocation* pix-loc)
        series1 [0.1 0.2 0.3]
        series2 [0.10 0.11 0.12]
        ts-1 (thrift/TimeSeries* 827 series1)
        ts-2 (thrift/TimeSeries* 828 series2)
        pedigree1 1
        pedigree2 2
        src [[(thrift/DataChunk* "forma" loc ts-1 t-res :pedigree pedigree1)]
             [(thrift/DataChunk* "forma" loc ts-2 t-res :pedigree pedigree2)]]
        _ (p/to-pail probs-pail-path src)]
    (MergeProbs s-res t-res est-end probs-pail-path merged-path)
    (hfs-seqfile merged-path)) => (produces [(into pix-loc [827 [0.1 0.1 0.11 0.12]])]))

(fact
  "Test everything after preprocessing"
  (let [tmp-root "/tmp/run-test"
        bad-pix-loc [s-res 28 8 0 1]
        really-bad-pix-loc [s-res 28 8 0 2]
        loc (apply thrift/ModisPixelLocation* pix-loc)
        bad-loc (apply thrift/ModisPixelLocation* bad-pix-loc)
        really-bad-loc (apply thrift/ModisPixelLocation* really-bad-pix-loc)
        ndvi [6269 3115 3542 4885 3088 5780 1987 4961 5367 5645 6926 6254 8017 1591 9045 3502 6807 3108 95 4808 689 4925 8934 4019 44 3409 5599 31 2240 4185 8251 6402 6610 252 7432 9661 1980 2544 7034 7540 1212 376 1600 8949 8880 7486 5145 6025 173 497 7555 4785 1354 550 796 5020 4757 5558 8664 1940 4127 115 620 1783 9401 2393 2359 1111 3477 2589 3204 4816 7416 5401 3158 1043 4641 9848 500 2122 9168 3335 4193 9417 7676 3272 4024 6537 314 8806 5435 9746 556 7241 5264 6625 92 4602 9264 4009 809 7434 2202 4103 2235 3432 6548 3240 9461 1783 6373 1278 3538 182 8904 805 3605 4440 7370 4471 7609 8296 5302 5591 6353 5599 4204 1903 3855 4386 3672 4837 5486 2533 5451 7380]
        rain [7 4 7 0 8 4 9 2 0 2 3 5 2 2 8 3 8 5 1 4 8 2 7 4 1 1 4 1 1 5 5 7 9 3 5 9 3 2 2 6 5 3 2 9 8 8 1 3 3 4 1 9 7 2 8 0 2 7 9 8 3 8 7 4 2 6 4 8 9 3 1 6 3 8 3 7 0 6 4 4 2 0 7 0 3 9 9 4 2 0 2 8 1 1 6 7 1 0 1 6 6 6 9 4 6 8 4 1 7 8 8 2 3 2 2 2 3 5 1 4 2 7 6 0 6 9 1 0 5 5 2 9 6 6 8 9]
        static [25 14000 1000 100 20]
        bad-static [25 14000 1000 100 0] ;; coast
        really-bad-static [0 14000 1000 0 0] ;; low vcf, coast
        ndvi-src [[(thrift/DataChunk* "ndvi" loc (thrift/TimeSeries* start-idx ndvi) t-res)]
                  [(thrift/DataChunk* "ndvi" bad-loc (thrift/TimeSeries* start-idx ndvi) t-res)]
                  [(thrift/DataChunk* "ndvi" really-bad-loc (thrift/TimeSeries* start-idx ndvi) t-res)]]
        rain-src [(into pix-loc [start-idx rain])
                  (into bad-pix-loc [start-idx rain])
                  (into really-bad-pix-loc [start-idx rain])]
        len (count ndvi)
        fire-src [(conj pix-loc (sample-fire-series start-idx len))
                  (conj bad-pix-loc (sample-fire-series start-idx len))
                  (conj really-bad-pix-loc (sample-fire-series start-idx len))]
        static-src [(vec (concat pix-loc static))
                    (vec (concat bad-pix-loc bad-static))
                    (vec (concat really-bad-pix-loc really-bad-static))]
        ndvi-path (.getPath (io/temp-dir "ndvi-path"))
        rain-path (.getPath (io/temp-dir "rain-path"))
        static-path (.getPath (io/temp-dir "static-path"))
        fire-path (.getPath (io/temp-dir "fire-path"))
        _ (?- (hfs-seqfile ndvi-path :sinkmode :replace) ndvi-src)
        _ (?- (hfs-seqfile rain-path :sinkmode :replace) rain-src)
        _ (?- (hfs-seqfile static-path :sinkmode :replace) static-src)
        _ (?- (hfs-seqfile fire-path :sinkmode :replace)  fire-src)]
    

    (workflow [tmp-root]

              ndviseries
              ([:tmp-dirs ndvi-series-path]
                 (TimeseriesFilter s-res t-res ndvi-path static-path ndvi-series-path))

              adjustseries
              ([:tmp-dirs adjust-series-path]
                 (AdjustSeries s-res t-res ndvi-series-path rain-path adjust-series-path))

              trends
              ([:tmp-dirs trends-path]
                 (Trends s-res t-res est-end adjust-series-path trends-path))

              trends-pail
              ([:tmp-dirs trends-pail-path]
                 (TrendsPail s-res t-res est-end trends-path trends-pail-path))
              
              merge-trends
              ([:tmp-dirs merge-trends-path]
                 (MergeTrends s-res t-res est-end trends-pail-path merge-trends-path))
              
              formatap
              ([:tmp-dirs forma-tap-path]
                 (FormaTap s-res t-res est-start est-end fire-path merge-trends-path forma-tap-path))

              neighborquery
              ([:tmp-dirs neighbor-path]
                 (NeighborQuery s-res t-res forma-tap-path neighbor-path))

              betadataprep
              ([:tmp-dirs beta-data-path]
                 (BetaDataPrep s-res t-res neighbor-path static-path beta-data-path))

              genbetas
              ([:tmp-dirs beta-path]
                 (GenBetas s-res t-res est-start beta-data-path beta-path))

              estimateforma
              ([:tmp-dirs probs-path]
                 (EstimateForma s-res t-res beta-path neighbor-path static-path probs-path))

              probs-pail
              ([:tmp-dirs probs-pail-path]
                 (ProbsPail s-res t-res est-end probs-path probs-pail-path))
              
              merge-probs
              ([:tmp-dirs merge-probs-path]
                 (MergeProbs s-res t-res est-end probs-pail-path merge-probs-path))))
  1
  => 1)
