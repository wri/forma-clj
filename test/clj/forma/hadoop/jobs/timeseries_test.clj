(ns forma.hadoop.jobs.timeseries-test
  (:use forma.hadoop.jobs.timeseries
        cascalog.api
        [midje sweet cascalog])
  (:require [forma.hadoop.io :as io]
            [forma.schema :as schema]
            [forma.thrift :as thrift]
            [forma.date-time :as date]))

(defn test-chunks
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, vector>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [dataset periods chunk-size]
  (for [period (range periods)
        :let [date (date/period->datetime "32" period)
              location (thrift/ModisChunkLocation* "1000" 8 6 0 chunk-size)
              chunk (into [] (range chunk-size))]]
    ["path" (thrift/DataChunk* dataset location chunk "32" date)]))

(defn test-fires
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, vector>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [start-pd periods]
  (for [period (range start-pd (+ start-pd periods))
        :let [date (date/period->datetime "1" period)
              location (thrift/ModisPixelLocation* "500" 28 8 0 0)
              tuple (thrift/FireValue* 1 1 1 1)]]
    [(thrift/DataChunk* "fire" location tuple "16" :date date)]))

(fact
  "Test `timeseries` query"
  (let [src [[1 (thrift/pack (range 10))] [3 (thrift/pack (range 10))]]
        src (<- [?pix-idx ?t-start ?t-end ?series]
                (src ?date ?ts)
                ((timeseries -9999.0) ?date ?ts :> ?pix-idx ?t-start ?t-end ?series))]
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
        ((extract-tseries *missing-val* src) ?dc)
        (thrift/unpack ?dc :> ?name ?loc ?data-val ?t-res _ _)
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

(def START-DAY
  "2000-01-01 at daily resolution"
  (date/datetime->period "1" "2000-01-01"))

(def START-PERIOD
  "Corresponds to period starting 2000-01-01 at 16-day resolution"
  (date/datetime->period "16" (date/period->datetime "1" START-DAY)))

(def SERIES-LENGTH
  "Length of fire series, in days"
  30)

(def T-RES
  "Temporal resolution"
  "16")

(def NUM-PERIODS
  "Length of fire-series"
  4)

(fact
  "Test aggregate-fires"
  (let [src (test-fires START-DAY SERIES-LENGTH)]
    (aggregate-fires src T-RES))
  => (produces [["fire" "2000-01-01" "500" 28 8 0 0 (thrift/FireValue* 16 16 16 16)]
                ["fire" "2000-01-17" "500" 28 8 0 0 (thrift/FireValue* 14 14 14 14)]]))

(fact
  "Test sum-fire-series"
  (let [src (test-fires START-DAY SERIES-LENGTH)]
    (-> (aggregate-fires src T-RES)
        (sum-fire-series START-PERIOD NUM-PERIODS T-RES)))
  => (let [TS (thrift/TimeSeries* START-PERIOD [(thrift/FireValue* 16 16 16 16)
                                                (thrift/FireValue* 30 30 30 30)
                                                (thrift/FireValue* 30 30 30 30)
                                                (thrift/FireValue* 30 30 30 30)])]
       (produces [["fire" "500" 28 8 0 0 TS]])))

(fact
  "Test create-fire-series. Should return a monotonically increasing
   series."
  (let [t-res "16"
        start-idx 709
        fire-start "2000-11-01"
        start-day (date/datetime->period "1" fire-start)
        end-date (date/todays-date)
        today (date/datetime->period "1" (date/todays-date)) ;; day number
        len (- today start-day)
        src (test-fires start-day len)
        [start end data] (->> (create-fire-series src t-res fire-start end-date)
                              ??-
                              flatten
                              first
                              thrift/unpack
                              (#(nth % 2))
                              thrift/unpack)
        fire-series (thrift/unpack data)
        today-period (date/datetime->period t-res (date/todays-date))
        first-fire-count (first (thrift/unpack (first (thrift/unpack data))))
        last-fire-count (first (thrift/unpack (last (thrift/unpack data))))]
    today-period => end
    first-fire-count => 15
    last-fire-count => len))
