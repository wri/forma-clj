(ns forma.hadoop.jobs.api-test
  "We’ll generate an API master dataset after each update. This
  dataset will be vertically partitioned into sequence files using a
  template tap as follows:

  /api/{iso}/{year}"
  (:use cascalog.api
        [midje sweet cascalog])
  (:use forma.hadoop.jobs.api :reload)
  (:require [cascalog.ops :as c]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.utils :as u]))

(def est-map {:nodata -9999.0 :est-start "2005-12-19" :t-res "16"})

(def sample-forma-data
  [["500" 28 8 0 0 827 [0.5 0.49 0.55 0.54 0.60 0.59] 88500]
   ["500" 28 8 0 1 827 [0. 0. 0. 0. 0. 0. 0.] 88500]])

(def sample-static-data
  [["500" 28 8 0 0 88114]])

(fact "Test `wide->long`"
  (let [start-idx 827
        src [[1 [1 2 3]]]]
    (<- [?id ?pd ?val]
        (src ?id ?series)
        (wide->long start-idx ?series :> ?pd ?val)))
  => (produces [[1 827 1] [1 828 2] [1 829 3]]))

(fact "Check `prob-series->tsv-api`. Note that output probabilities
       are smoothed. The second test includes a threshold."
  (prob-series->tsv-api est-map sample-forma-data)
  => (produces-some
      [[9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2005-12-19" "2005" 50]
       [9.99374999999999 101.542824160711 "IDN" "IDN" 88500 "2006-02-02" "2006" 0]])
  (let [thresh 50]
    (prob-series->tsv-api est-map sample-forma-data thresh)
    => (produces [[9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2005-12-19" "2005" 50]
                  [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-01-01" "2006" 50]
                  [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-01-17" "2006" 51]
                  [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-02-02" "2006" 53]
                  [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-02-18" "2006" 56]
                  [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-03-06" "2006" 58]])))


(fact "Check `mk-tsv`"
  (let [forma-path "/tmp/forma"
        out-path "/tmp/out"]
    (?- (hfs-seqfile forma-path :sinkmode :replace) sample-forma-data) 
    (mk-tsv est-map forma-path out-path)
    (hfs-textline (str out-path "/IDN/2006"))
    => (produces-some [["9.99791666666666\t101.54412568476158\tIDN\t88500\t2006-01-01\t50"]])))
