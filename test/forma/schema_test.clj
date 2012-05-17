(ns forma.schema-test
  "This namespace provides unit testing coverage for the forma.schema namespace."
  (:use forma.schema
        [midje sweet cascalog])
  (:require [forma.date-time :as date])
  (:import [forma.schema
            ArrayValue
            DataChunk
            DataValue
            DoubleArray
            FireSeries
            FireTuple
            FormaSeries
            FormaValue
            IntArray
            LocationProperty
            LongArray
            ModisChunkLocation
            ModisPixelLocation
            NeighborValue
            NeighborValue
            ShortArray]))

(tabular
 (fact
   "Test the unpacker multimethod."
   (unpacker ?t-value) => ?value)
 ?t-value ?value
 (ShortArray. [1 2 3]) [1 2 3]
 (ShortArray. []) []
 (IntArray. [1 2 3]) [1 2 3]
 (IntArray. []) []
 (LongArray. [1 2 3]) [1 2 3]
 (LongArray. []) []
 (DoubleArray. [1 2 3]) [1 2 3]
 (DoubleArray. []) [])

(tabular
 (fact
   "Test thrifter multimethod."
   (thrifter ?type ?args) => ?obj)
 ?type ?args ?obj
 Short -32768 (DataValue/shortVal -32768)
 Short 32767 (DataValue/shortVal 32767)
 Short -32769 (throws Exception)
 Short 32768 (throws Exception)
 Integer -2147483648 (DataValue/intVal -2147483648)
 Integer 2147483647 (DataValue/intVal 2147483647)
 Integer -2147483649 (throws Exception)
 Integer 2147483648 (throws Exception)
 Long -9223372036854775808 (DataValue/longVal -9223372036854775808)
 Long 9223372036854775807 (DataValue/longVal 9223372036854775807)
 Long -9223372036854775809 (throws Exception)
 Long -9223372036854775808 (throws Exception)
 Double 1 (DataValue/doubleVal 1))

;; (tabular
;;  (fact
;;    "Test unpack method: (unpack (FireTuple. 1 2 3 4)) => [1 2 3 4]"
;;    (unpack ?thrift-obj ?keys) => ?vals)
;;  ?thrift-obj ?keys ?vals
;;  (FireTuple. 11 22 33 44) [] [11 22 33 44]
;;  (FireTuple. 11 22 33 44) [:temp330] [11]
;;  (FireTuple. 11 22 33 44) [:conf50] [22]
;;  (FireTuple. 11 22 33 44) [:bothPreds] [33]
;;  (FireTuple. 11 22 33 44) [:count] [44]
;;  (FireTuple. 11 22 33 44) [:temp330 :conf50 :bothPreds :count] [11 22 33 44]
;;  (FireTuple. 11 22 33 44) [:count :bothPreds :conf50 :temp330] [44 33 22 11])


