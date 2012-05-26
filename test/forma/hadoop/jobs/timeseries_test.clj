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
  (for [period (range periods)
        :let [date (d/period->datetime "1" period)
              location (thrift/ModisPixelLocation* "1000" sample 6 10 10)
              tuple (thrift/FireValue* 1 1 1 1)]]
    ["path" (thrift/DataChunk* "fire" location tuple "32" date)]))

(future-fact?-
 "Add in test for results, here! Add another test for the usual
 aggregate-fires business.

Also note that we're testing for truncation after march."
 "this results vector needs an overhaul:"
 [[1] [2]]
 (??- (-> (vec (concat (test-fires 4 100)
                       (test-fires 10 100)))
          (create-fire-series "32" "1970-01-01" "1970-03-01"))))

(future-fact
 "Need to update this -- we want to check that the results of
this query don't contain -9999."
 (let [results (-> (concat (test-chunks "precl" 10 1200)
                           (test-chunks "ndvi" 10 1200))
                   (vec)
                   (extract-tseries -9999)
                   (??-)
                   (first))]
   (second results) =not=> "something about not containing -9999."))

(fact
  "Test running-fire-sum"
  (let [fires-src [[[(schema/fire-value 0 0 0 10)
                     (schema/fire-value 1 0 0 33)]]]
        result [(schema/fire-series 0 [(schema/fire-value 0 0 0 10)
                                      (schema/fire-value 1 0 0 43)])]]
    (??<- [?vals]
        (fires-src ?fire-vals)
        (:distinct false)
        (running-fire-sum 0 ?fire-vals :> ?vals)) => [result]))
