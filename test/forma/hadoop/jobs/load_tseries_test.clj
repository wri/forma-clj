(ns forma.hadoop.jobs.load-tseries-test
  (:use forma.hadoop.jobs.load-tseries
        clojure.test
        midje.sweet)
  (:require [forma.hadoop.io :as io]))

(defn test-chunks
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, int-array>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [periods chunk-size]
  (for [period (range periods)]
    (vector period
            (io/int-struct chunk-size
                           (range chunk-size)))))
