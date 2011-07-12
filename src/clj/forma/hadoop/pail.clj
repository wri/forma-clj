(ns forma.hadoop.pail
  (:use cascalog.api
        [forma.source.modis :only (hv->tilestring)])
  (:import [forma.schema
            DataChunk FireTuple FormaValue LocationProperty LocationPropertyValue
            ModisPixelLocation DataValue]
           [backtype.hadoop.pail Pail]))

(gen-class :name forma.hadoop.pail.DataChunkPailStructure
           :extends forma.tap.ThriftPailStructure
           :prefix "pail-")

(defn pail-getType [this] DataChunk)
(defn pail-createThriftObject [this] (new DataChunk))

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

(defn mk-chunk
  [dataset t-res date s-res mh mv sample line val]
  (doto (DataChunk. dataset
                    (->> (ModisPixelLocation. s-res mh mv sample line)
                         LocationPropertyValue/pixelLocation
                         LocationProperty.)
                    (DataValue/intVal val)
                    t-res)
    (.setDate date)))

(def my-pail
  (let [path "/tmp/pail"]
    (try (Pail. path)
         (catch Exception e
           (Pail/create path (new forma.hadoop.pail.SplitDataChunkPailStructure))))))

(defn write-tuples [dataset m-res t-res]
  (with-open [stream (.openWrite my-pail)]
    (try (doseq [x (range 1000)]
           (.writeObject stream (mk-chunk dataset t-res "2005-12-01" m-res 8 6 x 1 10)))
         (finally (.consolidate my-pail)))))
