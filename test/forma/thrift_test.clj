(ns forma.thrift-test
  "Unit test coverage for the thrift namespace. In particular, check creating,
  accessing, and unpacking Thrift objects defined in the dev/forma.thrift IDL."
  (:use forma.thrift
        [midje sweet cascalog])
  (:import [forma.schema
            ArrayValue DataChunk DataValue DoubleArray FireArray
            FireValue FormaValue IntArray LocationProperty
            LocationPropertyValue LongArray ModisChunkLocation
            ModisPixelLocation ShortArray TimeSeries FormaArray
            NeighborValue]
           [org.apache.thrift TBase TUnion]
           [java.util ArrayList]))

(fact "Check creating and unpacking NeighborValue objects."
  (NeighborValue* (FireValue. 1 1 1 1) 1 2.0 3.0 4.0 5.0 6.0 7.0) =>
  (NeighborValue. (FireValue. 1 1 1 1) 1 2.0 3.0 4.0 5.0 6.0 7.0)
  
  (unpack (NeighborValue. (FireValue. 1 1 1 1) 1 2.0 3.0 4.0 5.0 6.0 7.0)) =>
  [(FireValue. 1 1 1 1) 1 2.0 3.0 4.0 5.0 6.0 7.0 0.0 0.0]

  (let [x (NeighborValue. (FireValue. 1 1 1 1) 1 2.0 3.0 4.0 5.0 6.0 7.0)]
    (doto x
      (.setAvgParamBreak 8.0)
      (.setMinParamBreak 9.0))
    (NeighborValue* (FireValue. 1 1 1 1) 1 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0) => x))

(fact "Check creating and unpacking FireValue objects."
  (FireValue* 1 1 1 1) => (FireValue. 1 1 1 1)
  (unpack (FireValue. 1 1 1 1)) => [1 1 1 1])

(fact "Check creating and unpacking ModisChunkLocation objects."
  (ModisChunkLocation* "500" 1 1 1 1) => (ModisChunkLocation. "500" 1 1 1 1)
  (ModisChunkLocation* "500" 1.0 1 1 1) => (throws AssertionError)
  (ModisChunkLocation* "500" "1" 1 1 1) => (throws AssertionError)
  (unpack (ModisChunkLocation* "500" 1 1 1 1)) => ["500" 1 1 1 1])

(fact "Check creating and unpacking ModisPixelLocation objects."
  (ModisPixelLocation* "500" 1 1 1 1) => (ModisPixelLocation. "500" 1 1 1 1)
  (ModisPixelLocation* "500" 1.0 1 1 1) => (throws AssertionError)
  (ModisPixelLocation* "500" "1" 1 1 1) => (throws AssertionError)
  (unpack (ModisPixelLocation* "500" 1 1 1 1)) => ["500" 1 1 1 1])

(fact "Check creating and unpacking TimeSeries objects."
  (TimeSeries* 0 3 [1 1 1 1]) => (TimeSeries. 0 3 (->> (vec (map int [1 1 1 1])) IntArray. ArrayValue/ints))
  (TimeSeries* 0 [1 1 1 1]) => (TimeSeries. 0 3 (->> (vec (map int [1 1 1 1])) IntArray. ArrayValue/ints))
  
  (unpack (TimeSeries* 0 1 [1 1 1 1])) =>
  [0 1 (->> (map int [1 1 1 1]) IntArray. ArrayValue/ints)])

(fact "Check creating and unpacking FormaValue objects."
  (FormaValue* (FireValue. 1 1 1 1) 1.0 2.0 3.0) =>
  (FormaValue. (FireValue. 1 1 1 1) 1.0 2.0 3.0)

  (let [x  (FormaValue. (FireValue. 1 1 1 1) 1.0 2.0 3.0)]
    (doto x
      (.setParamBreak 4.0))
    (FormaValue* (FireValue. 1 1 1 1) 1.0 2.0 3.0 4.0) => x)

  (unpack (FormaValue* (FireValue. 1 1 1 1) 1.0 2.0 3.0)) =>
  [(FireValue. 1 1 1 1) 1.0 2.0 3.0 0.0])

(fact "Check creating and unpacking DataChunk objects."
  (let [loc (->> (ModisChunkLocation. "500" 8 0 100 24000)
                 LocationPropertyValue/chunkLocation
                 LocationProperty.)
        data (->> (vec (map int [1 1 1 1])) IntArray. ArrayValue/ints DataValue/vals)
        x (DataChunk. "name" loc data "16")
        c (DataChunk* "name" (ModisChunkLocation. "500" 8 0 100 24000) [1 1 1 1] "16" "2001")]
    (doto x
      (.setDate "2001"))
    c => x
    (unpack c) => ["name" loc data "16" "2001"]))

