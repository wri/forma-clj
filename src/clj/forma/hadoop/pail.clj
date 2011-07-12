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

;; ## Taps

(gen-class :name forma.hadoop.pail.DataChunkPailTap
           :extends backtype.cascading.tap.PailTap
           :init init
           :constructors {[String]
                          [String backtype.cascading.tap.PailTap$PailTapOptions]
                          [String Object]
                          [String backtype.cascading.tap.PailTap$PailTapOptions]
                          [String Object Object]
                          [String backtype.cascading.tap.PailTap$PailTapOptions]}
           :prefix "tap-")

(defn tap-init
  "Accepts a base pail path and a sequence of "
  ([s] (tap-init s nil))
  ([s coll] (tap-init s coll (forma.hadoop.pail.DataChunkPailStructure.)))
  ([s coll structure]
     [[s (-> (PailTap/makeSpec nil structure)
             (PailTap$PailTapOptions. "datachunk" (into-array java.util.List coll) nil))]
      nil]))

(defn tap-getSpecificStructure [this]
  (forma.hadoop.pail.DataChunkPailStructure.))

(gen-class :name forma.hadoop.pail.SplitDataChunkPailTap
           :extends forma.hadoop.pail.DataChunkPailTap
           :init init
           :constructors {[String] [String Object Object]
                          [String Object] [String Object Object]
                          [String Object Object] [String Object Object]}
           :prefix "splittap-")

(defn splittap-init
  "Accepts a base pail path and a sequence of "
  ([s] (splittap-init s nil (forma.hadoop.pail.SplitDataChunkPailStructure.)))
  ([s coll] (splittap-init s coll (forma.hadoop.pail.SplitDataChunkPailStructure.)))
  ([s coll structure] [[s coll structure] nil]))

(defn splittap-getSpecificStructure [this]
  (forma.hadoop.pail.SplitDataChunkPailStructure.))
