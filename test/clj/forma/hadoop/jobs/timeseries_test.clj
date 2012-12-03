(ns forma.hadoop.jobs.timeseries-test
  (:use forma.hadoop.jobs.timeseries
        cascalog.api
        [midje sweet cascalog])
  (:require [forma.hadoop.io :as io]
            [forma.schema :as schema]
            [forma.thrift :as thrift]            
            [forma.date-time :as d]))

(defn test-chunks
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, vector>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [dataset periods chunk-size]
  (for [period (range periods)
        :let [date (d/period->datetime "32" period)
              location (thrift/ModisChunkLocation* "1000" 8 6 0 chunk-size)
              chunk (into [] (range chunk-size))]]
    ["path" (thrift/DataChunk* dataset location chunk "32" date)]))

(defn test-fires
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, vector>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [sample periods]
  (let [pd 13111] ;; 2005-12-03 at daily resolution
    (for [period (range pd (+ pd periods))
        :let [date (d/period->datetime "1" period)
              location (thrift/ModisPixelLocation* "500" sample 6 10 10)
              tuple (thrift/FireValue* 1 1 1 1)]]
    [(thrift/DataChunk* "fire" location tuple "16" :date date)])))

(future-fact?-
 "Add in test for results, here! Add another test for the usual
  aggregate-fires business.

  Also note that we're testing for truncation after march."
 [[1] [2]]
 (??- (-> (vec (concat (test-fires 4 100)
                       (test-fires 10 100)))
          (create-fire-series "32" "1970-01-01" "1970-03-01"))))

(future-fact
  "Need to update this -- we want to check that the results of this
  query don't contain -9999."
 (let [results (-> (concat (test-chunks "precl" 10 1200)
                           (test-chunks "ndvi" 10 1200))
                   (vec)
                   (extract-tseries -9999)
                   (??-)
                   (first))]
   (second results) =not=> "something about not containing -9999."))

(fact
  "Test `timeseries` query"
  (let [src [[1 (thrift/pack (range 10))] [3 (thrift/pack (range 10))]] 
        src (<- [?pix-idx ?t-start ?t-end ?series]
                (src ?date ?ts)
                (timeseries [-9999.0] ?date ?ts :> ?pix-idx ?t-start ?t-end ?series))]
    (<- [?pix-idx ?t-start ?t-end ?ts-vec]
        (src ?pix-idx ?t-start ?t-end ?series)
        (thrift/unpack* ?series :> ?ts-vec)))
  => (produces [[0 1 3 [0 -9999 0]]
                [1 1 3 [1 -9999 1]]
                [2 1 3 [2 -9999 2]]
                [3 1 3 [3 -9999 3]]
                [4 1 3 [4 -9999 4]]
                [5 1 3 [5 -9999 5]]
                [6 1 3 [6 -9999 6]]
                [7 1 3 [7 -9999 7]]
                [8 1 3 [8 -9999 8]]
                [9 1 3 [9 -9999 9]]]))

(fact
  "Test form-tseries"
  (let [mk-ts (form-tseries -9999.0)
        src [["16" "2000-01-01" (thrift/pack [1 2 3])]
             ["16" "2000-02-02" (thrift/pack [2 3 4])]]]
    (<- [?idx ?start ?end ?series]
          (src ?t-res ?date ?ts)
          (mk-ts ?t-res ?date ?ts :> ?idx ?start ?end ?series)))
  => (produces [0 690 692 (thrift/pack [1 -9999 2])]
               [1 690 692 (thrift/pack [2 -9999 3])]
               [2 690 692 (thrift/pack [3 -9999 4])]))

(fact
  "Test extract-tseries"
  (let [loc (thrift/ModisChunkLocation* "500" 28 8 0 24000)
        data (range 24000)
        mk-data #(thrift/DataChunk* "ndvi" loc data "16" :date %)
        src [["" (mk-data "2000-01-01")]
             ["" (mk-data "2000-02-02")]]]
    (<- [?name ?s-res ?mod-h ?mod-v ?sample ?line ?start ?end ?t-res ?series]
        ((extract-tseries src *missing-val*) ?dc)
        (thrift/unpack ?dc :> ?name ?loc ?data-val ?t-res _)
        (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
        (thrift/unpack ?data-val :> ?start ?end ?series-arr)
        (thrift/unpack* ?series-arr :> ?series)))
  => (produces-some [["ndvi" "500" 28 8 2399 9 690 692 "16" [23999 -9999 23999]]]))

(fact
  "Test running-fire-sum"
  (let [fires-src [[[(thrift/FireValue* 0 0 0 10)
                     (thrift/FireValue* 1 0 0 33)]]]
        result [[(thrift/TimeSeries* 0 [(thrift/FireValue* 0 0 0 10)
                                       (thrift/FireValue* 1 0 0 43)])]]]
    (<- [?vals]
        (fires-src ?fire-vals)
        (running-fire-sum 0 ?fire-vals :> ?vals)) => (produces result)))

(fact
  "Test aggregate-fires"
  (let [src (test-fires 1 30)]
    (aggregate-fires src "16"))
  => (let []
       (produces [["fire" "2005-12-03" "500" 1 6 10 10 (thrift/FireValue* 16 16 16 16)]
                  ["fire" "2005-12-19" "500" 1 6 10 10 (thrift/FireValue* 13 13 13 13)]
                  ["fire" "2006-01-01" "500" 1 6 10 10 (thrift/FireValue* 1 1 1 1)]])))

(fact
  "Test create-fire-series. Should return a monotonically increasing
   series truncated to 2006-01-01"
  (let [src (test-fires 1 100)]
    (create-fire-series src "16" "2000-11-01" "2005-12-19" "2006-01-01"))
  => (let [loc (thrift/ModisPixelLocation* "500" 1 6 10 10)
           FS (thrift/TimeSeries* 827 [(thrift/FireValue* 29 29 29 29)
                                       (thrift/FireValue* 30 30 30 30)])]
       (produces [[(thrift/DataChunk* "fire" loc FS "16")]])))
