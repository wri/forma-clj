;; This file isolates all direct interaction with Hadoop, and provides
;; access to any custom schemes used by FORMA. Any new schemes, or
;; low-level additions to our hadoop interaction capabilities should
;; go here, allowing our other components to work at a high level of
;; abstraction.

(ns forma.hadoop.io
  (:use cascalog.api
        forma.schema
        [clojure.string :only (join)])
  (:require [cascalog.workflow :as w]
            [forma.utils :as u]
            [forma.reproject :as r]
            [forma.date-time :as date])
  (:import [forma WholeFile]
           [forma.schema FireSeries FormaValue  DataChunk LocationProperty
            LocationPropertyValue DataValue TimeSeries
            ModisPixelLocation ModisChunkLocation]
           [java.util ArrayList]
           [cascading.tuple Fields]))

;; ## Custom File Input
;;
;; Hadoop is optimized for massive files -- enormous website logs,
;; huge bodies of text, lists of friend relations on twitter, that
;; sort of stuff. Hadoop jobs require a defined input format for each
;; job, [as described here](http://goo.gl/YX2Ol). The input format
;; generates a number of [Input Splits](http://goo.gl/0UWKd); these
;; input splits are byte streams, and require a custom [record
;; reader](http://goo.gl/AmVJB) for conversion to key-value pairs. The
;; text record reader, for example, would convert a 64MB chunk of
;; lines into <k, v> pairs of <byte-offset, line> within the actual
;; input split. Most word counting examples ignore this key.
;;
;; The data from each input split goes to a single mapper. From the
;; wonderful [Yahoo! tutorial](http://goo.gl/u2hOe); "By default, the
;; FileInputFormat and its descendants break a file up into 64 MB
;; chunks (the same size as blocks in HDFS)." By splitting a file on
;; block size, Hadoop allows processing on huge documents to scale
;; linearly. Twice as many bytes flows into twice as many mappers, so
;; doubling the nodes should keep processing time the same.
;;
;; Now, this is all well and good for massive text files; as long as
;; one doesn't split in the middle of a line, the sizes of the chunks
;; are arbitrary. With certain compressed file formats, the picture
;; changes a bit. NASA's MODIS files are compressed using HDF4. A cut
;; in the middle of one of these files would result in meaningless
;; data, as neither half can be decompressed without the other.
;;
;; To deal with this issue, we've written a custom WholeFile input
;; format, to guarantee that input splits will break along file
;; boundaries. The custom WholeFileRecordReader takes one of these
;; input splits and converts it into a <k, v> pair of <filename,
;; file-bytes>. The file's bytes are wrapped in a Hadoop
;; [BytesWritable](http://goo.gl/GVP3e) object. (It should be noted
;; that for small files, this method will be very inefficient, as
;; we're overriding Hadoop's ability to leap across files and stick to
;; its optimal 64MB input split size.)
;;
;; MODIS files range from 24 to 48 MB, so all is well, but a more
;; efficient general solution can be obtained by deriving
;; WholeFileInputFormat from
;; [CombineFileInputFormat](http://goo.gl/awr4T). In this scenario, An
;; input split would still break along file boundaries, but might
;; include multiple files. The record reader would have to be adjusted
;; to produce multiple <k,v> pairs for every input split.
;;
;; As [Cascalog](http://goo.gl/SRmDh) allows us to write cascading
;; jobs (rather than straight MapReduce), we had to define a custom
;; WholeFile [scheme](http://goo.gl/Doggg) to be able to take
;; advantage of [cascading's Tap abstraction](http://goo.gl/RNMLT) for
;; data sources. Once we had WholeFile.java, the clojure wrapper
;; became trivial.

(defn whole-file
  "Custom scheme for dealing with entire files."
  [field-names]
  (WholeFile. (w/fields field-names)))

;; ## Cascading Taps
;;
;; Another helpful feature provided by cascading is the ability to
;; pair a scheme with a special [HFS class](http://goo.gl/JHpNT), that
;; allows access to a number of different file systems. A tap acts
;; like a massive vacuum cleaner. Open up a tap in a directory, and
;; the tap inhales everything inside of it, passing it in as food for
;; whatever scheme it's associated with. HFS can deal with HDFS, local
;; fileystem, and Amazon S3 bucket paths; A path prefix of,
;; respectively, `hdfs://`, `file://`, and `s3n://` forces the proper
;; choice.

(defn hfs-wholefile
  "Subquery to return distinct files in the supplied directory. Files
  will be returned as 2-tuples, formatted as `<filename, file>` The
  filename is a text object, while the entire, unchopped file is
  encoded as a Hadoop `BytesWritable` object."
  [path & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (whole-file))]
    (apply hfs-tap scheme path opts)))

;; ## Bucket to Cluster
;;
;; To get tuples back out of our directory structure on S3, we employ
;; Cascading's [GlobHFS] (http://goo.gl/1Vwdo) tap, along with an
;; interface tailored for datasets stored in the MODIS sinusoidal
;; projection. For details on the globbing syntax, see
;; [here](http://goo.gl/uIEzu).
;;
;; GlobHfs is activated through the `:source-pattern` argument to one
;; of the hfs- taps.

(defn tiles->globstring
  [& tiles]
  {:pre [(r/valid-modis? tiles)]}
  (->> (for [[th tv] tiles]
         (format "h%02dv%02d" th tv))
       (join "," )
       (format "*{%s}*")))

(defn globstring
  "Takes a path ending in `/` and collections of nested
  subdirectories, and returns a globstring formatted for cascading's
  GlobHFS. (`*` may be substituted in for any argument but path.)

    Example Usage:
    (globstring \"s3n://bucket/\" [\"ndvi\" \"evi\"] [\"1000-32\"] *)
    ;=> \"s3://bucket/{ndvi,evi}/{1000-32}/*\"

    (globstring \"s3n://bucket/\" * * [\"008006\" \"033011\"])
    ;=> \"s3://bucket/*/*/{008006,033011}\""
  [& pieces]
  (->> pieces
       (map (fn [x]
              (cond (= * x) "*"
                    (coll? x) (format "{%s}" (join "," x))
                    :else x)))
       (join "/")))

;; ### Collections

(defprotocol Thriftable
  (to-struct [xs]))

(defn struct-edges
  "Accepts a sequence of pairs of initial integer time period and
  Thrift timeseries objects (or sequences), and returns the maximum
  start period and the minimum end period. For example:

    (struct-edges [0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]"
  [pair-seq]
  {:pre [(even? (count pair-seq))]}
  (reduce (fn [[lo hi] [x0 ct]]
            [(max lo x0) (min hi ct)])
          (for [[x0 seq] (partition 2 pair-seq)]
            [x0 (+ x0 (count seq))])))

(defn adjust
  "Appropriately truncates the incoming Thrift timeseries
  structs (paired with the initial integer period), and outputs a new
  start and both truncated series. For example:

    (adjust 0 [1 2 3 4] 1 [2 3 4 5])
    ;=> (1 (to-struct [2 3 4]) (to-struct [2 3 4]))"
  [& pairs]
  {:pre [(even? (count pairs))]}
  (let [[bottom top] (struct-edges pairs)]
    (cons bottom
          (for [[x0 seq] (partition 2 pairs)]
            (trim-seq bottom top x0 seq)))))

(defn forma-schema
  "Accepts a number of timeseries of equal length and starting
  position, and converts the first entry in each timeseries to a forma
  value, for all first values and on up the sequence. Series must be
  supplied as specified by the arguments for `forma-value`. For
  example:

    (forma-schema fire-series short-series long-series t-stat-series)

  `fire-series` must be an instance of `FireSeries`. `short-series`
  and `long-series` must be instances of `IntSeries` or
  `DoubleSeries`."
  [& in-series]
  [(->> in-series
        (map #(if % (get-vals %) (repeat %)))
        (apply map forma-value)
        (to-struct))])

;; ## DataValue Generation

(defn chunk-location
  [s-res mod-h mod-v idx size]
  (->> (ModisChunkLocation. s-res mod-h mod-v idx size)
       LocationPropertyValue/chunkLocation
       LocationProperty.))

(defn pixel-location
  [s-res mh mv sample line]
  (->> (ModisPixelLocation. s-res mh mv sample line)
       LocationPropertyValue/pixelLocation
       LocationProperty.))

(defn get-start-idx [^TimeSeries ts]
  (.getStartIdx ts))

(defn get-location-property
  [^DataChunk chunk]
  (.getLocationProperty chunk))

(defn extract-location
  [^DataChunk chunk]
  (-> chunk
      .getLocationProperty
      .getProperty
      .getFieldValue))

(defn extract-chunk-value
  [^DataChunk chunk]
  (-> chunk
      .getChunkValue
      .getFieldValue))

(defn extract-dataset
  [^DataChunk chunk]
  (.getDataset chunk))

(defn extract-date
  [^DataChunk chunk]
  (.getDate chunk))

(defn extract-ts-data
  "Used by timeseries. Returns `[dataset-name t-res date collection]`,
   where collection is a vector."
  [^DataChunk chunk]
  [(extract-dataset chunk)
   (.getTemporalRes chunk)
   (extract-date chunk)
   (extract-location chunk)
   (extract-chunk-value chunk)])

(defn get-pos
  [^ModisPixelLocation loc]
  [(.getResolution loc)
   (.getTileH loc)
   (.getTileV loc)
   (.getSample loc)
   (.getLine loc)])

(defn expand-pos
  "TODO: Rename this bastard."
  [^ModisChunkLocation loc pix-idx]
  (let [m-res      (.getResolution loc)
        chunk-size (.getChunkSize loc)
        chunk-idx  (.getChunkID loc)]
    (apply vector m-res
           (.getTileH loc)
           (.getTileV loc)
           (r/tile-position m-res chunk-size chunk-idx pix-idx))))

(defn chunkloc->pixloc
  "Used by timeseries for conversion."
  [loc pix-idx]
  (apply pixel-location (expand-pos loc pix-idx)))

;; The following are used in timeseries.
(defn swap-location
  [^DataChunk chunk location]
  (doto chunk (.setLocationProperty location)))

(defn swap-data
  [^DataChunk chunk data-value]
  (doto chunk (.setChunkValue data-value)))

(defn set-date
  [^DataChunk chunk date]
  (doto chunk (.setDate date)))

(defn set-temporal-res
  [^DataChunk chunk t-res]
  (doto chunk (.setTemporalRes t-res)))

(defn adjust-timeseries
  "Takes in any number of thrift TimeSeries objects, and returns a new
  sequence of appropriately truncated TimeSeries objects."
  [& tseries]
  (let [[start & ts-seq] (->> tseries
                              (mapcat (juxt :start-idx :series))
                              (apply adjust))]
    (map (partial timeseries-value start)
         ts-seq)))

(defn adjust-fires
  "Returns the section of fires data found appropriate based on the
  information in the estimation parameter map."
  [{:keys [est-start est-end t-res]} ^FireSeries f-series]
  (let [f-start (.getStartIdx f-series)
        [start end] (for [pd [est-start est-end]]
                      (date/datetime->period "32" pd))]
    [(->> (: f-series)
          (u/trim-seq start (inc end) f-start)
          (timeseries-value start))]))

(defn mk-chunk
  [dataset t-res date location data-value]
  (let [chunk {:temporal-res t-res
               :location     location
               :dataset      dataset
               :value        data-value}]
    (if-not date
      chunk
      (assoc chunk :date date))))
