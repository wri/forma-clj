(ns forma.hadoop.jobs.timeseries-test
  (:use forma.hadoop.jobs.timeseries
        cascalog.api
        [midje sweet cascalog])
  (:require [forma.hadoop.io :as io]
            [forma.schema :as schema]
            [forma.date-time :as d]))

(defn test-chunks
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, vector>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [dataset periods chunk-size]
  (for [period (range periods)
        :let [date     (d/period->datetime "32" period)
              location (schema/chunk-location "1000" 8 6 0 chunk-size)
              chunk    (into [] (range chunk-size))]]
    ["path" (schema/chunk-value dataset "32" date location chunk)]))

(defn test-fires
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, vector>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [sample periods]
  (for [period (range periods)
        :let [date     (d/period->datetime "1" period)
              location (schema/pixel-location "1000" sample 6 10 10)
              tuple    (schema/fire-value 1 1 1 1)]]
    ["path" (schema/chunk-value "fire" "32" date location tuple)]))

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
