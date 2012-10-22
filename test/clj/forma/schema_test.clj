(ns forma.schema-test
  "Tests for most functions in forma.schema.  So far, the tests for
  the fundamental, building-block functions are indirectly tested
  through the tests of the higher-order functions (which call the
  lower-order functions)."
  (:use forma.schema
        [midje sweet cascalog])
  (:require [forma.date-time :as date]
            [forma.thrift :as thrift])
  (:import [forma.schema
            ArrayValue DataChunk DataValue DoubleArray FireArray
            FireValue FormaValue IntArray LocationProperty
            LocationPropertyValue LongArray ModisChunkLocation
            ModisPixelLocation ShortArray TimeSeries FormaArray
            NeighborValue]
           [org.apache.thrift TBase TUnion]))

(defn- f-series
  "Returns a fire series of length 10 with starting index `start-idx`
  for testing."
  [start-idx]
  (thrift/TimeSeries* start-idx (repeat 10 (thrift/FireValue* 0 0 0 0))))

(facts
  "Test that fire sequence is appropriately trimmed by
 `adjust-fires`.  Note that the interval length defined by :est-start
 and :est-end amounts to 5 periods."

  ;; Test for appropriate trimming over both ends of the interval
  (let [est-map {:est-start "2005-12-31" :est-end "2006-03-01" :t-res "16"}
        [_ _ arr] (apply thrift/unpack (adjust-fires est-map (f-series 826)))]
    (count (thrift/unpack arr)) => 5)

  ;; Test for overhang only over the tail-end of the interval
  (let [est-map {:est-start "2005-12-31" :est-end "2006-03-01" :t-res "16"}
        [_ _ arr] (apply thrift/unpack (adjust-fires est-map (f-series 830)))]
    (count (thrift/unpack arr)) => 2))

(def neighbors
  "Create a small vector of FormaValues indicating that they are
  neighbors; used for testing that the neighbor values are
  appropriately merged and combined."
  [(thrift/FormaValue* (thrift/FireValue* 1 1 1 1) 1. 2. 3. 4.)
   (thrift/FormaValue* (thrift/FireValue* 2 1 1 2) 2. 3. 4. 5.)
   (thrift/FormaValue* (thrift/FireValue* 2 1 1 2) 3. 4. -9999.0 6.)])

(facts
  "Checks that `combine-neighbors` functions properly,
   even with a `nodata` value embedded in the neighbor values."
  (let [nodata (double -9999)]
    (combine-neighbors nodata neighbors))
  => (neighbor-value (thrift/FireValue* 3 2 2 3) 2
                                                 1.5 1.
                                                 2.5 2.
                                                 3.5 3.
                                                 4.5 4.)
  (let [neighbors [(thrift/FormaValue* (thrift/FireValue* 2 1 1 2) -9999.0 4. -9999.0 6.)
                   (thrift/FormaValue* (thrift/FireValue* 2 1 1 2) -9999.0 4. -9999.0 6.)]
        nodata (double -9999)]
    (combine-neighbors nodata neighbors)) => empty-neighbor-val)

(fact "boundaries testing."
  (boundaries [ 0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]
  (boundaries [ 0 [1 2 3 4] [2 3 4 5]]) => (throws AssertionError))

(tabular
 (fact "sequence adjustment testing."
   (adjust ?a0 ?a-vec ?b0 ?b-vec) => [?start ?a ?b])
 ?a0 ?a-vec    ?b0 ?b-vec    ?start ?a      ?b
 0   [1 2 3 4] 1   [2 3 4 5] 1      [2 3 4] [2 3 4]
 2   [9 8 7]   0   [1 2 3 4] 2      [9 8]   [3 4]
 10  [2 3 4]   1   [1 2 3]   10     []      [])

(fact "Test that the first element of each of the supplied timeseries
  to `forma-seq` are appropriately bundled into the first FormaValue
  of the output timeseries (consisting of FormaValues for each
  period)."
  (let [fire-1 (thrift/FireValue* 1 1 1 1)
        fire-0 (thrift/FireValue* 0 0 0 0)
        fire-series (thrift/TimeSeries* 826 (repeat 5 fire-1))
        short-series  [0. 1. 2. 3. 4.]
        long-series   [1. 3. 5. 7. 9.]
        t-stat-series [2. 4. 6. 8. 10.]
        break-series  [10. 11. 12. 13. 14.]]

    ;; Test for an existing fire
    (ffirst
     (forma-seq fire-series short-series long-series t-stat-series break-series))
    => (thrift/FormaValue* fire-1 0. 1. 2. 10.)

    ;; Test for the case when there are no fires
    (ffirst
     (forma-seq nil short-series long-series t-stat-series break-series))
    => (thrift/FormaValue* fire-0 0. 1. 2. 10.)))
