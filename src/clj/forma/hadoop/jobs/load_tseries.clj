(ns forma.hadoop.jobs.load-tseries
  (:use cascalog.api
        [forma.date-time :only (datetime->period current-period)]
        [forma.source.modis :only (tile-position
                                   tilestring->hv)]
        [forma.hadoop.io :only (chunk-tap)]
        [forma.static :only (chunk-size)]
        [forma.trends :only (timeseries)]
        [forma.trends.analysis :only (whoop-shell)])
  (:require [cascalog.ops :as c]
            [forma.hadoop.predicate :as p])
  (:gen-class))

(def
  ^{:doc "Predicate macro aggregator that extracts a timeseries, given
  `?chunk`, `?temporal-resolution` and `?date` dynamic vars."}
  extract-tseries
  (<- [?temporal-res ?date ?chunk :> ?pix-idx ?t-start ?t-end ?tseries]
      (datetime->period ?temporal-res ?date :> ?tperiod)
      (:sort ?tperiod)
      (timeseries ?tperiod ?int-chunk :> ?pix-idx ?t-start ?t-end ?tseries)))

(def
  ^{:doc "Predicate macro function to extract relevant spatial data
  from a given set of chunk fields."}
  extract-attrs
  (<- [?tilestring ?spatial-res ?chunk-size ?chunkid ?pix-idx :> ?tile-h ?tile-v ?sample ?line]
      (tilestring->hv ?tilestring :> ?tile-h ?tile-v)
      (tile-position ?spatial-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defn process-tseries
  "Given a source of chunks, this subquery generates timeseries with
  all relevant accompanying information."
  [chunk-source]
  (<- [?dataset ?spatial-res ?temporal-res ?tile-h ?tile-v ?sample ?line ?t-start ?t-end ?tseries]
      (chunk-source ?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?int-chunk)
      (count ?int-chunk :> ?chunk-size)
      (extract-tseries ?temporal-res ?date ?int-chunk :> ?pix-idx ?t-start ?t-end ?tseries)
      (extract-attrs ?tilestring ?spatial-res ?chunkid ?pix-idx :> ?tile-h ?tile-v ?sample ?line)))

(def ndvi1 [7417 7568 7930 8049 8039 8533 8260 8192 7968 7148 7724
            8800 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538
            7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698
            7838 8967 8136 7532 7838 8009 8136 8400 8219 8051 8091
            7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813
            8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216
            7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195
            8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041
            7250 7884 8105 8033 8340 8288 7691 7599 8480 8563 8033
            7708 7575 7996 7739 8058 7400 6682 7999 7655 7533 7904
            8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878
            8524 8167 7526 7330 7325 7485 8108 7978 7035 7650])

(def ndvi2 [7417 7568 7930 8049 8039 8533 8260 8192 1968 7148 7724
            8811 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538
            7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698
            7838 8967 8136 7532 7438 8009 8136 8400 8219 8051 8091
            7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813
            8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216
            7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195
            8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041
            7250 7884 8105 8033 8140 8288 7691 7599 8480 8563 8033
            7708 7575 7996 7739 7058 7400 3682 7999 2655 7533 7904
            8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878
            8524 8167 7526 7330 2325 2485 2108 2978 2035 7650])

(def test-data [["ndvi" "1000" "32" 8 6 100 100 361 491 ndvi1]
                ["ndvi" "1000" "32" 8 6 100 101 361 491 ndvi2]])

(defn tester-casc [data]
  (let [ref (int (datetime->period "32" "2005-12-01"))]
    (?<- (stdout)
         [?h ?v ?s ?l ?refdata]
         (data ?dset ?s-res ?t-res ?h ?v ?s ?l ?t-start ?t-end ?tseries)
         (whoop-shell 15 5 ref ref ?t-end ?t-start ?t-end ?tseries :> ?refdata ?estdata))))

(def options-map
  {:ref-date  "2005-12-01"
   :est-start "2005-12-01"
   :est-end   "2010-12-01"
   :t-res     "32"
   :long-block 15
   :window     5})

(defn mk-transform
  [{:keys [ref-date t-res]}]
  (let [ref (int (datetime->period t-res ref-date))]
    (<- [?t-start ?t-end ?tseries :> ?refdata ?estdata]
        (whoop-shell 15 5 ref ref ?t-end ?t-start ?t-end ?tseries :> ?refdata ?estdata))))

(defn tester-casc
  [data]
  (?<- (stdout)
       [?h ?v ?s ?l ?refdata]
       (data ?dset ?s-res ?t-res ?h ?v ?s ?l ?t-start ?t-end ?t-series)
       (whoop-shell ?t-start ?t-end ?t-series options-map :> ?refdata ?estdata)))

(defn -main
  "TODO: Docs.

  Sample usage:

      (-main \"s3n://redddata/\" \"/timeseries/\" \"ndvi\"
             \"1000-32\" [\"008006\" \"008009\"])"
  [base-input-path output-path & pieces]
  (let [pieces (map read-string pieces)
        chunk-source (apply chunk-tap base-input-path pieces)]
    (?- (hfs-seqfile output-path)
        (process-tseries chunk-source))))
