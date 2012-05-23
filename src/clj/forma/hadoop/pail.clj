(ns forma.hadoop.pail
  (:use cascalog.api
        [cascalog.io :only (with-fs-tmp)]
        [forma.reproject :only (hv->tilestring)])
  (:import [java.util List]
           [forma.schema DataChunk FormaValue
            LocationProperty LocationPropertyValue
            ModisPixelLocation DataValue]
           [backtype.cascading.tap PailTap PailTap$PailTapOptions]
           [backtype.hadoop.pail PailStructure Pail]
           [forma.tap ThriftPailStructure]))

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
  (let [location   (-> d .getLocationProperty .getProperty .getFieldValue)
        resolution (format "%s-%s"
                           (.getResolution location)
                           (.getTemporalRes d))]
    [(.getDataset d) resolution]))

(defn split-isValidTarget [this dirs]
  (boolean (#{2 3} (count dirs))))

(defn pail-structure []
  (forma.hadoop.pail.SplitDataChunkPailStructure.))

;; ## Pail Taps

(defn- pail-tap
  [path colls structure]
  (let [seqs (into-array List colls)
        spec (PailTap/makeSpec nil structure)
        opts (PailTap$PailTapOptions. spec "!datachunk" seqs nil)]
    (PailTap. path opts)))

(defn split-chunk-tap [path & colls]
  (pail-tap path colls (pail-structure)))

;; TODO: If the pail doesn't exist, rather than providing
;; pail-structure, pull the structure information out of the tap.

(defn ?pail-*
  "Executes the supplied query into the DataChunkPailStructure pail
  located at the supplied path, consolidating when finished."
  [tap pail-path query]
  (let [pail (Pail/create pail-path (pail-structure) false)]
    (with-fs-tmp [_ tmp]
      (?- (tap tmp) query)
      (.absorb pail (Pail. tmp)))))

;; TODO: This makes the assumption that the pail-tap is being created
;; in the macro call. Fix this by swapping the temporary path into the
;; actual tap vs destructuring.

(defmacro ?pail-
  "Executes the supplied query into the pail located at the supplied
  path, consolidating when finished."
  [[tap path] query]
  (list `?pail-* tap path query))

(defn to-pail
  "Executes the supplied `query` into the pail at `pail-path`. This
  pail must make use of the `DataChunkPailStructure`."
  [pail-path query]
  (?pail- (split-chunk-tap pail-path)
          query))

(defmain consolidate [pail-path]
  (.consolidate (Pail. pail-path)))

(defmain absorb [from-pail to-pail]
  (.absorb (Pail. to-pail)
           (Pail. from-pail)))
