(ns forma.hadoop.pail
  (:import [forma.schema
            DataChunk FireTuple FormaValue LocationProperty LocationPropertyValue
            ModisPixelLocation DataValue]
           [backtype.hadoop.pail Pail]
           [forma.tap SplitDataChunkPailStructure]))

(defn mk-chunk [dataset s-res tilestring m-res]
  (doto (DataChunk. "ndvi" "32" "008006"
                    (->> (ModisPixelLocation. "1000" 1 1 2 3)
                         LocationPropertyValue/pixelLocation
                         LocationProperty.)
                    (DataValue/intVal 10))
    (.setDate "2005-12-01")))
