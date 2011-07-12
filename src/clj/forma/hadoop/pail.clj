(ns forma.hadoop.pail
  (:use [forma.source.modis :only (hv->tilestring)])
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
  (let [location (-> d .getLocationProperty .getProperty .getPixelLocation)
        tilestring (hv->tilestring (.getTileH location) (.getTileV location))
        res (format "%s-%s"
                    (.getResolution location)
                    (.getTemporalRes d))]
    [(.getDataset d) res tilestring]))


(defn split-isValidTarget [this dirs]
  (boolean (#{3 4} (count dirs))))
