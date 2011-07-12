(ns forma.hadoop.pail-io
  (:use cascalog.api)
  (:require [forma.hadoop.io :as io])
  (:import [backtype.hadoop.pail Pail]
           [forma.schema DataChunk FireTuple FormaValue
            LocationProperty LocationPropertyValue
            ModisPixelLocation DataValue]
           [forma.hadoop.pail DataChunkPailTap SplitDataChunkPailTap
            DataChunkPailStructure SplitDataChunkPailStructure]))

;; TODO: Move o
(defn pail-tap
  "Returns a tap into the guts of the supplied pail."
  [path]
  (SplitDataChunkPailTap. path))

(defn mk-data-value
  [val type]
  (case type
        :int-struct    (DataValue/ints (io/int-struct val))
        :int           (DataValue/intVal val)
        :double-struct (DataValue/doubles (io/double-struct val))
        :double        (DataValue/doubleVal val)))

(defn mk-chunk
  [dataset t-res date s-res mh mv sample line data-value]
  (doto (DataChunk. dataset
                    (->> (ModisPixelLocation. s-res mh mv sample line)
                         LocationPropertyValue/pixelLocation
                         LocationProperty.)
                    data-value
                    t-res)
    (.setDate date)))
