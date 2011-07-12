(ns forma.hadoop.pail-io
  (:use cascalog.api
        [cascalog.io :only (with-fs-tmp)])
  (:require [forma.hadoop.io :as io])
  (:import [backtype.hadoop.pail Pail]
           [backtype.cascading.tap PailTap PailTap$PailTapOptions]
           [forma.schema DataChunk LocationProperty ModisPixelLocation DataValue]
           [forma.hadoop.pail DataChunkPailStructure SplitDataChunkPailStructure]))

;; ## Taps

(defn pail-tap
  [s colls structure]
  (let [seqs (into-array java.util.List colls)
        spec (PailTap/makeSpec nil structure)
        opts (PailTap$PailTapOptions. spec "datachunk" seqs nil)]
    (PailTap. s opts)))

(defn data-chunk-tap [s & colls]
  (pail-tap s colls (DataChunkPailStructure.)))

(defn split-chunk-tap [s & colls]
  (pail-tap s colls (SplitDataChunkPailStructure.)))

(defn ?pail-*
  "Executes the supplied query into the pail located at the supplied
  path, consolidating when finished."
  [tap pail-path query]
  (let [pail (Pail. pail-path)]
    (with-fs-tmp [_ tmp]
      (?- (tap tmp) query)
      (.absorb pail (Pail. tmp))
      (.consolidate pail))))

(defmacro ?pail-
  "Executes the supplied query into the pail located at the supplied
  path, consolidating when finished."
  [[tap path] query]
  (list `?pail-* tap path query))

;; ## DataValue Generation

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
