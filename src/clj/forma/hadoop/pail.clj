(ns forma.hadoop.pail
  (:use cascalog.api
        [cascalog.io :only (with-fs-tmp)]
        [forma.source.modis :only (hv->tilestring)])
  (:import [forma.schema DataChunk FireTuple FormaValue
            LocationProperty LocationPropertyValue
            ModisPixelLocation DataValue]
           [backtype.cascading.tap PailTap PailTap$PailTapOptions]
           [backtype.hadoop.pail Pail]))

;; ## Pail Data Structures

(gen-class :name forma.hadoop.pail.DataChunkPailStructure
           :extends forma.tap.ThriftPailStructure
           :prefix "pail-")

(defn pail-getType [this] DataChunk)
(defn pail-createThriftObject [this] (DataChunk.))

(gen-class :name forma.hadoop.pail.SplitDataChunkPailStructure
           :extends forma.hadoop.pail.DataChunkPailStructure
           :prefix "split-")

(defn split-getTarget [this ^DataChunk d]
  (let [location (-> d .getLocationProperty .getProperty .getFieldValue)
        tilestring (hv->tilestring (.getTileH location) (.getTileV location))
        res (format "%s-%s"
                    (.getResolution location)
                    (.getTemporalRes d))]
    [(.getDataset d) res tilestring]))


(defn split-isValidTarget [this dirs]
  (boolean (#{3 4} (count dirs))))

;; ## Pail Taps

(defn pail-tap
  [s colls structure]
  (let [seqs (into-array java.util.List colls)
        spec (PailTap/makeSpec nil structure)
        opts (PailTap$PailTapOptions. spec "datachunk" seqs nil)]
    (PailTap. s opts)))

(defn data-chunk-tap [s & colls]
  (pail-tap s colls (forma.hadoop.pail.DataChunkPailStructure.)))

(defn split-chunk-tap [s & colls]
  (pail-tap s colls (forma.hadoop.pail.SplitDataChunkPailStructure.)))

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
