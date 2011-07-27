;; This file isolates all direct interaction with Hadoop, and provides
;; access to any custom schemes used by FORMA. Any new schemes, or
;; low-level additions to our hadoop interaction capabilities should
;; go here, allowing our other components to work at a high level of
;; abstraction.

(ns forma.hadoop.io
  (:use cascalog.api
        [clojure.string :only (join)]
        [cascalog.tap :only (hfs-tap)])
  (:require [forma.source.modis :as m]
            [cascalog.workflow :as w]
            [forma.utils :as u]
            [forma.date-time :as date])
  (:import [forma WholeFile]
           [forma.schema DoubleArray IntArray
            FireTuple FireSeries FormaValue FormaSeries
            FormaNeighborValue DataChunk LocationProperty
            LocationPropertyValue DataValue ArrayValue TimeSeries
            ModisPixelLocation ModisChunkLocation]
           [java.util ArrayList]
           [cascading.tuple Fields]
           [org.apache.hadoop.io BytesWritable]))

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
  (let [scheme (-> (:outfields opts Fields/ALL)
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
  {:pre [(m/valid-modis? tiles)]}
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

;; ## BytesWritable Interaction
;;
;; For schemes that specifically deal with Hadoop BytesWritable
;; objects, we provide the following methods to abstract away the
;; confusing java details. (For example, while a BytesWritable object
;; wraps a byte array, not all of the bytes returned by the getBytes
;; method are valid. As mentioned in the
;; [documentation](http://goo.gl/3qzyc), "The data is only valid
;; between 0 and getLength() - 1.")

(defn hash-str
  "Generates a unique identifier for the supplied BytesWritable
  object. Useful as a filename, when worried about clashes."
  [^BytesWritable bytes]
  (-> bytes .hashCode Math/abs str))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object. As
  mentioned in the [BytesWritable javadoc](http://goo.gl/cjjlD), only
  the first N bytes are valid, where N = `(.getLength byteswritable)`."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

;; ## Thrift Wrappers

(defn list-of
  "Maps `f` across all entries in `xs`, and returns the result wrapped
  in an instance of `java.util.ArrayList`."
  [f xs]
  (ArrayList. (for [x xs]
                (try (f x)
                     (catch Exception e nil)))))

;; ### Fire Tuples

(defn fire-tuple
  "Clojure wrapper around the java `FireTuple` constructor."
  [t-above-330 c-above-50 both-preds count]
  (FireTuple. t-above-330 c-above-50 both-preds count))

;; TODO: Add this extract fields business to the protocol below, and
;; implement it for this and the forma value extraction.
(defn extract-fields
  "Returns a vector containing the value of the `temp330`, `conf50`,
  `bothPreds` and `count` fields of the supplied `FireTuple` thrift
  object."
  [firetuple]
  [(.getTemp330 firetuple)
   (.getConf50 firetuple)
   (.getBothPreds firetuple)
   (.count firetuple)])

(defn add-fires
  "Returns a new `FireTuple` object generated by summing up the fields
  of each of the supplied `FireTuple` objects."
  [& f-tuples]
  (->> f-tuples
       (map extract-fields)
       (apply map +)
       (apply fire-tuple)))

;; ### Forma Values

(defn forma-value
  [fire short long t-stat]
  (FormaValue. (or fire (fire-tuple 0 0 0 0)) short long t-stat))

(defn unpack-forma-val
  "Returns a persistent vector containing the `FireTuple`, short drop,
  long drop and t-stat fields of the supplied `FormaValue`."
  [^FormaValue forma-val]
  [(.getFireValue forma-val)
   (.getShortDrop forma-val)
   (.getLongDrop forma-val)
   (.getTStat forma-val)])

(defn neighbor-value
  ([forma-val]
     (let [[fire short long t] (unpack-forma-val forma-val)]
       (neighbor-value fire 1 short short long long t t)))
  ([fire neighbors avg-short min-short avg-long min-long avg-stat min-stat]
     (FormaNeighborValue. fire neighbors
                          avg-short min-short
                          avg-long min-long
                          avg-stat min-stat)))

(defn unpack-neighbor-val
  [^FormaNeighborValue neighbor-val]
  [(.getFireValue neighbor-val)
   (.getNumNeighbors neighbor-val)
   (.getAvgShortDrop neighbor-val)
   (.getMinShortDrop neighbor-val)
   (.getAvgLongDrop neighbor-val)
   (.getMinLongDrop neighbor-val)
   (.getAvgTStat neighbor-val)
   (.getMinTStat neighbor-val)])

;; ## Neighbor Values

(defn merge-neighbor
  "Merges the supplied instance of `FormaValue` into the existing
  aggregate collection of `FormaValue`s represented by
  `neighbor-val`. (`neighbor-val` must be an instance of
  `FormaNeighborValue`."
  [neighbor-val forma-val]
  (let [[fire short long t] (unpack-forma-val forma-val)
        [n-fire ct short-mean short-min long-mean long-min t-mean t-min]
        (unpack-neighbor-val neighbor-val)]
    (FormaNeighborValue. (add-fires n-fire fire)
                         (inc ct)
                         (u/weighted-mean short-mean ct short 1)
                         (min short-min short)
                         (u/weighted-mean long-mean ct long 1)
                         (min long-min long)
                         (u/weighted-mean t-mean ct t 1)
                         (min t-min t))))

(defn combine-neighbors
  "Returns a `FormaNeighborValue` instances generated by merging
together each entry in the supplied sequence of `FormaValue`s."
  [[x & more]]
  (if x
    (reduce merge-neighbor (neighbor-value x) more)
    (neighbor-value (fire-tuple 0 0 0 0) 0 0 0 0 0 0 0)))

(defn textify
  "Converts the supplied coordinates, `FormaValue` and
  `FormaNeighborValue` into a line of text suitable for use in STATA."
  [^FormaValue val ^FormaNeighborValue neighbor-val]
  (let [[fire-val s-drop l-drop t-drop] (unpack-forma-val val)
        [fire-sum ct short-mean short-min
         long-mean long-min t-mean t-min] (unpack-neighbor-val neighbor-val)
        [k330 c50 ck fire] (extract-fields fire-val)
        [k330-n c50-n ck-n fire-n] (extract-fields fire-sum)]
    (join " "
          [k330 c50 ck fire s-drop l-drop t-drop
           k330-n c50-n ck-n fire-n
           ct short-mean short-min long-mean long-min t-mean t-min])))

;; ### Collections

(defn int-struct
  "Casts all numbers in the supplied sequence to ints, and returns
  them wrapped in an instance of `forma.schema.IntArray`."
  [xs]
  (let [ints (list-of int xs)]
    (doto (IntArray.)
      (.setInts ints))))

(defn double-struct
  "Casts all numbers in the supplied sequence to doubles, and returns
  them wrapped in an instance of `forma.schema.DoubleArray`."
  [xs]
  (let [doubles (list-of double xs)]
    (doto (DoubleArray.)
      (.setDoubles doubles))))

(defn fire-series
  "Creates a `FireSeries` object from the supplied sequence of
  `FireTuple` objects."
  ([start xs]
     (fire-series start
                  (dec (+ start (count xs)))
                  xs))
  ([start end xs]
     (when (seq xs)
       (doto (FireSeries.)
         (.setValues (ArrayList. xs))
         (.setStartIdx start)
         (.setEndIdx end)))))

(defn forma-series
  "Creates a `FormaSeries` object from the supplied sequence of
  `FormaValue` objects."
  [xs]
  (doto (FormaSeries.)
    (.setValues (ArrayList. xs))))

(defprotocol Thriftable
  (get-vals [x])
  (count-vals [x])
  (to-struct [xs]))

(extend-protocol Thriftable
  java.lang.Iterable
  (to-struct [[v :as xs]]
    (cond (= forma.schema.FormaValue (type v)) (forma-series xs)
          (integer? v) (int-struct xs)
          (float? v) (double-struct xs)))
  (get-vals [x] x)
  (count-vals [x] (count x))

  TimeSeries
  (get-vals [x] (get-vals
                 (.getFieldValue (.getSeries x))))
  (count-vals [x]
    (count (get-vals x)))
  
  FireSeries
  (get-vals [x] (.getValues x))
  (count-vals [x]
    (count (get-vals x)))

  FormaSeries
  (get-vals [x] (.getValues x))
  (count-vals [x]
    (count (get-vals x)))

  IntArray
  (get-vals [x] (.getInts x))
  (count-vals [x]
    (count (get-vals x)))

  DoubleArray
  (get-vals [x] (.getDoubles x))
  (count-vals [x]
    (count (get-vals x))))

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
            [x0 (+ x0 (count-vals seq))])))

(defn trim-struct
  "Trims a Thriftable struct with initial value indexed at x0 to fit
  within bottom (inclusive) and top (exclusive). For example:

    (trim-struct 0 2 0 [1 2 3]) => (to-struct [0 1 2])"
  [bottom top x0 seq]
  (->> seq
       (get-vals)
       (u/trim-seq bottom top x0)
       (to-struct)))

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
            (trim-struct bottom top x0 seq)))))

(defn forma-schema
  "Accepts a number of timeseries of equal length and starting
  position, and converts the first entry in each timeseries to a
  `FormaValue`, for all first values and on up the sequence. Series
  must be supplied as specified by the arguments for
  `forma-value`. For example:

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

(defmulti mk-array-value class)
(defmethod mk-array-value IntArray [x] (ArrayValue/ints x))
(defmethod mk-array-value DoubleArray [x] (ArrayValue/doubles x))

(defmulti mk-data-value class)
(defmethod mk-data-value IntArray [x] (DataValue/ints x))
(defmethod mk-data-value Integer [x] (DataValue/intVal x))
(defmethod mk-data-value DoubleArray [x] (DataValue/doubles x))
(defmethod mk-data-value Double [x] (DataValue/doubleVal x))
(defmethod mk-data-value FireTuple [x] (DataValue/fireVal x))
(defmethod mk-data-value TimeSeries [x] (DataValue/timeSeries x))
(defmethod mk-data-value FireSeries [x] (DataValue/fireSeries x))

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
   where collection is `IntArray` or `DoubleArray`."
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
  (let [m-res (.getResolution loc)
        chunk-size (.getChunkSize loc)
        chunk-idx (.getChunkID loc)]
    (apply vector m-res
           (.getTileH loc)
           (.getTileV loc)
           (m/tile-position m-res chunk-size chunk-idx pix-idx))))

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

(defn timeseries-value
  "TODO: get rid of the whens, here."
  ([start ^ArrayValue series]
     (when series
       (let [elems (-> series
                       .getFieldValue
                       count-vals)]
         (timeseries-value start
                           (dec (+ start elems))
                           series))))
  ([start end series]
     (when series
       (TimeSeries. start end series))))

(defn adjust-timeseries
  "Takes in any number of thrift TimeSeries objects, and returns a new
  sequence of appropriately truncated TimeSeries objects."
  [& tseries]
  (let [[start & ts-seq] (->> tseries
                              (mapcat (juxt #(.getStartIdx %) get-vals))
                              (apply adjust))]
    (map (comp (partial timeseries-value start)
               #(when % (mk-array-value %)))
         ts-seq)))

(defn adjust-fires
  "Returns the section of fires data found appropriate based on the
  information in the estimation parameter map."
  [{:keys [est-start est-end t-res]} ^FireSeries f-series]
  (let [f-start (.getStartIdx f-series)
        [start end] (for [pd [est-start est-end]]
                      (date/datetime->period "32" pd))]
    [(->> (get-vals f-series)
          (u/trim-seq start (inc end) f-start)
          (fire-series start))]))

(defn mk-chunk
  [dataset t-res date location-prop data-value]
  (let [chunk (DataChunk. dataset
                          location-prop
                          data-value
                          t-res)]
    (if date
      (doto chunk (.setDate date))
      chunk)))
