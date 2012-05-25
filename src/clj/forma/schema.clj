(ns forma.schema
  (:require [clojure.string :as s]
            [forma.utils :as u]
            [forma.reproject :as r]
            [forma.date-time :as date])
  (:import [java.util ArrayList]
           [forma.schema
            ArrayValue DataChunk DataValue DoubleArray FireArray
            FireValue FormaValue IntArray LocationProperty
            LocationPropertyValue LongArray ModisChunkLocation
            ModisPixelLocation ShortArray TimeSeries FormaArray
            NeighborValue]
           [org.apache.thrift TBase TUnion]))

;; ## DataValue Generation

;; TODO:
;;
;; Fix this up
;; Add mk-data-value to chunkify
;;
(defmulti mk-array-value class)
(defmethod mk-array-value IntArray [x] (ArrayValue/ints x))
(defmethod mk-array-value DoubleArray [x] (ArrayValue/doubles x))
(defmethod mk-array-value LongArray [x] (ArrayValue/longs x))
(defmethod mk-array-value ShortArray [x] (ArrayValue/shorts x))
(defmethod mk-array-value FireArray [x] (ArrayValue/fires x))
(defmethod mk-array-value FormaArray [x] (ArrayValue/formas x))

(defmulti mk-data-value class)
(defmethod mk-data-value Double [x] (DataValue/doubleVal x))
(defmethod mk-data-value Integer [x] (DataValue/intVal x))
(defmethod mk-data-value Long [x] (DataValue/longVal x))
(defmethod mk-data-value Short [x] (DataValue/shortVal x))
(defmethod mk-data-value FireValue [x] (DataValue/fireVal x))
(defmethod mk-data-value TimeSeries [x] (DataValue/timeSeries x))
(defmethod mk-data-value ArrayValue [x] (DataValue/vals x))
(defmethod mk-data-value FormaValue [x] (DataValue/forma x))

;; ### Collections

(defn list-of
  "Maps `f` across all entries in `xs`, and returns the result wrapped
  in an instance of `java.util.ArrayList`."
  [f xs]
  (ArrayList. (for [x xs]
                (try (f x)
                     (catch Exception e nil)))))

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

(defn forma-array
  "Creates a `FormaArray` object from the supplied sequence of
  `FormaValue` objects."
  [xs]
  (doto (FormaArray.)
    (.setValues (ArrayList. xs))))

(defn fire-array
  "Creates a `FormaArray` object from the supplied sequence of
  `FormaValue` objects."
  [xs]
  (doto (FireArray.)
    (.setFires (ArrayList. xs))))

(defprotocol Thriftable
  (to-struct [x])
  (get-vals [x]))

(extend-protocol Thriftable
  java.lang.Iterable
  (to-struct [[v :as xs]]
    (def cake xs)
    (cond (= forma.schema.FormaValue (type v)) (forma-array xs)
          (= forma.schema.FireValue (type v))  (fire-array xs)
          (integer? v)                         (int-struct xs)
          (float? v)                           (double-struct xs)))
  
  (get-vals [x] x)

  TimeSeries
  (get-vals [x] (get-vals (.getFieldValue (.getSeries x))))
  
  FormaArray
  (get-vals [x] (.getValues x))

  ShortArray
  (get-vals [x] (.getShorts x))

  IntArray
  (get-vals [x] (.getInts x))

  LongArray
  (get-vals [x] (.getLongs x))

  DoubleArray
  (get-vals [x] (.getDoubles x))

  FireArray
  (get-vals [x] (.getFires x))

  ArrayValue
  (get-vals [x] (get-vals (.getFieldValue x))))

(defn count-vals [x]
  (count (get-vals x)))

(defn boundaries
  "Accepts a sequence of pairs of <initial time period, collection>
  and returns the maximum start period and the minimum end period. For
  example:

    (boundaries [0 [1 2 3 4] 1 [2 3 4 5]]) => [1 4]"
  [pair-seq]
  {:pre [(even? (count pair-seq))]}
  (reduce (fn [[lo hi] [x0 ct]]
            [(max lo x0) (min hi ct)])
          (for [[x0 seq] (partition 2 pair-seq)]
            [x0 (+ x0 (count seq))])))

(defn adjust
  "Appropriately truncates the incoming timeseries values (paired with
  the initial integer period), and outputs a new start and both
  truncated series. For example:

    (adjust 0 [1 2 3 4] 1 [2 3 4 5])
    ;=> (1 [2 3 4] [2 3 4])"
  [& pairs]
  {:pre [(even? (count pairs))]}
  (let [[bottom top] (boundaries pairs)]
    (cons bottom
          (for [[x0 seq] (partition 2 pairs)]
            (into [] (u/trim-seq bottom top x0 seq))))))

;; ## Time Series

(defn timeseries-value
  "Takes a period index > 0 `start-idx` and a time series `series`,
 outputs same plus `end-idx` calculated by counting the number of
 elements in `series`.

    (timeseries-value 0 [1 2 3])
    ;=> {:start-idx 0
         :end-idx 2
         :series [1 2 3]}"
  ([start-idx ^ArrayValue series]
     (when series
       (let [elems (count-vals (.getFieldValue series))]
         (timeseries-value start-idx
                           (dec (+ start-idx elems))
                           series))))
  ([start-idx end-idx series]
     (when series
       (TimeSeries. start-idx end-idx series))))

;; ### Fire Values -- DONE

(defn fire-value
  "Creates a `fire-value` object with counts of fires meeting certain criteria:
    temp >= 330
    confidence >= 50
    both temp and confidence
    total # fires"
  [t-above-330 c-above-50 both-preds count]
  (FireValue. t-above-330 c-above-50 both-preds count))

(defn fire-series
  "Creates a `FireSeries` object from the supplied sequence of
  `FireValue` objects."
  ([start xs]
     (fire-series start
                  (dec (+ start (count xs)))
                  xs))
  ([start end xs]
     (when (seq xs)
       (doto (TimeSeries.)
         (.setSeries (ArrayValue/fires (FireArray. (ArrayList. xs))))
         (.setStartIdx start)
         (.setEndIdx end)))))

(defn extract-fire-fields
  "Returns a vector containing the value of the `temp330`, `conf50`,
  `bothPreds` and `count` fields of the supplied `FireValue` thrift
  object."
  [^FireValue val]
  [(.getTemp330 val)
   (.getConf50 val)
   (.getBothPreds val)
   (.count val)])

(defn add-fires
  "Returns a new `FireValue` object generated by summing up the fields
  of each of the supplied `FireValue` objects."
  [& f-tuples]
  (->> f-tuples
       (map extract-fire-fields)
       (apply map +)
       (apply fire-value)))

(defn adjust-fires
  "Returns the section of fires data found appropriate based on the
  information in the estimation parameter map."
  [{:keys [est-start est-end t-res]} ^TimeSeries f-series]
  (let [f-start (.getStartIdx f-series)
        [start end] (for [pd [est-start est-end]]
                      (date/datetime->period t-res pd))]
    [(->> (get-vals f-series)
          (u/trim-seq start (inc end) f-start)
          (fire-series start))]))

;; # Compound Objects

(defn forma-value
  "Returns a vector containing a FireValue, short-term drop,
  parametrized break, long-term drop and t-stat of the short-term
  drop."
  [fire short param-break long t-stat]
  (let [fire (or fire (fire-value 0 0 0 0))]
    [fire short param-break long t-stat]))

;; ## Neighbor Values

(defn neighbor-value
  "Accepts either a forma value or a sequence of sub-values."
  ([[fire short param long t-stat]]
     (doto (NeighborValue. fire 1 short short long long t-stat t-stat)
       (.setAvgParamBreak param)
       (.setMinParamBreak param)))
  ([fire neighbors avg-short
    min-short avg-long min-long avg-stat min-stat avg-param min-param]
     (doto (NeighborValue. fire neighbors avg-short min-short
                           avg-long min-long avg-stat min-stat)
       (.setAvgParamBreak avg-param)
       (.setMinParamBreak min-param))))

(defn unpack-neighbor-val
  [^NeighborValue neighbor-val]
  [(.getFireValue neighbor-val)
   (.getNumNeighbors neighbor-val)
   (.getAvgShortDrop neighbor-val)
   (.getMinShortDrop neighbor-val)
   (.getAvgLongDrop neighbor-val)
   (.getMinLongDrop neighbor-val)
   (.getAvgTStat neighbor-val)
   (.getMinTStat neighbor-val)
   (.getAvgParamBreak neighbor-val)
   (.getMinParamBreak neighbor-val)])

(defn merge-neighbors
  "Merges the supplied instance of `FormaValue` into the existing
  aggregate collection of `FormaValue`s represented by
  `neighbor-val`. (`neighbor-val` must be an instance of
  `NeighborValue`."
  [neighbor-val forma-val]
  (let [[fire short param long t] forma-val
        [n-fire ct short-mean short-min
         long-mean long-min t-mean t-min
         param-mean param-min]
        (unpack-neighbor-val neighbor-val)]
    (neighbor-value (add-fires n-fire fire)
                    (inc ct)
                    (u/weighted-mean short-mean ct short 1)
                    (min short-min short)
                    (u/weighted-mean long-mean ct long 1)
                    (min long-min long)
                    (u/weighted-mean t-mean ct t 1)
                    (min t-min t)
                    (u/weighted-mean param-mean ct param 1)
                    (min param-min param))))

(defn combine-neighbors
  "Returns a new forma neighbor value generated by merging together
   each entry in the supplied sequence of forma values."
  ;; TODO: come up with an example
  [[x & more]]
  (if x
    (reduce merge-neighbors (neighbor-value x) more)
    (neighbor-value (fire-value 0 0 0 0) 0 0 0 0 0 0 0 0 0)))

;; ## Location

(defn chunk-location
  [s-res mod-h mod-v idx size]
  (->> (ModisChunkLocation. s-res mod-h mod-v idx size)
       LocationPropertyValue/chunkLocation
       LocationProperty.))

(defn chunk-location
  "Creates a chunk-location object.

    (chunk-location \"1000\" 28 9 59 24000)
    ;=> {:spatial-res \"1000\", :mod-h 28, :mod-v 9, :index 59, :size 24000}"
  [spatial-res mod-h mod-v idx size]
  (->> (ModisChunkLocation. spatial-res mod-h mod-v idx size)
       LocationPropertyValue/chunkLocation
       LocationProperty.))

(defn pixel-location
  "Creates a pixel-location object.

    (pixel-location \"1000\" 28 9 1199 10)
    ;=> {:spatial-res \"1000\", :mod-h 28, :mod-v 9, :sample 1199, :line 10}"
  [spatial-res mod-h mod-v sample line]
  (->> (ModisPixelLocation. spatial-res mod-h mod-v sample line)
       LocationPropertyValue/pixelLocation
       LocationProperty.))

(defn unpack-pixel-location
  "Unpacks pixel-location object into a tuple.

    (unpack-pixel-location
     {:spatial-res \"1000\", :mod-h 28, :mod-v 9, :sample 1199, :line 10})
    ;=> [\"1000\" 28 9 1199 10]"
  [^ModisPixelLocation loc]
  [(.getResolution loc)
   (.getTileH loc)
   (.getTileV loc)
   (.getSample loc)
   (.getLine loc)])

(defn unpack-chunk-location
  "Unpacks chunk-location object into a tuple.

    (unpack-chunk-location
     {:spatial-res \"1000\", :mod-h 28, :mod-v 9, :index 4, :size 24000})
    ;=> [\"1000\" 28 9 4 24000]"
  [^ModisChunkLocation loc]
  [(.getResolution loc)
   (.getTileH loc)
   (.getTileV loc)
   (.getChunkID loc)
   (.getChunkSize loc)])

(defn chunkloc->pixloc
  "Accepts a chunk location and a pixel index within that location and
   returns a pixel location.
 
     (chunkloc->pixloc
      {:spatial-res \"1000\", :mod-h 28, :mod-v 9, :index 59, :size 24000}
      23999)

    ;=> {:spatial-res \"1000\", :mod-h 28, :mod-v 9, :sample 1199, :line 1199}"
  [chunk-loc pix-idx]
  (let [[spatial-res mod-h mod-v index size] (unpack-chunk-location chunk-loc)]
    (apply pixel-location spatial-res mod-h mod-v
           (r/tile-position spatial-res size index pix-idx))))

;; ## Data Chunks

(defn chunk-value
  "Creates a chunk-value object.

   Example:
    (let [size 24000
          location (chunk-location \"1000\" 8 6 0 24000)
          data (range size)]
     (chunk-value \"ndvi\" \"32\" \"2006-01-01\" location (take 10 data))

  ;=> {:date \"2006-01-01\"
       :temporal-res \"32\"
       :location {:spatial-res \"1000\", :mod-h 8, :mod-v 6, :index 0, :size 24000}     :dataset \"ndvi\"
       :value [0 1 2 3 4 5 6 7 8 9]}"
  [dataset t-res date location data-value]
  (let [chunk (DataChunk. dataset
                          location
                          data-value
                          t-res)]
    (if date
      (doto chunk
        (.setDate date))
      chunk)))

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

(defn unpack-chunk-val
  "Used by timeseries. Unpacks a chunk object. Returns `[dataset-name
 t-res date location collection]`, where collection is a vector and
 location is a `chunk-location`."
  [^DataChunk chunk]
  [(.getDataset chunk)
   (.getTemporalRes chunk)
   (.getDate chunk)
   (-> chunk .getLocationProperty .getProperty .getFieldValue)
   (-> chunk .getChunkValue .getFieldValue)])

(defn unpack-timeseries
  "Used by timeseries. Unpacks a chunk object. Returns `[dataset-name
 t-res date location collection]`, where collection is a vector and
 location is a `chunk-location`."
  [^TimeSeries ts]
  [(.getStartIdx ts)
   (.getEndIdx ts)
   (-> ts .getSeries .getFieldValue)])

(defn forma-seq
  "Accepts a number of timeseries of equal length and starting
  position, and converts the first entry in each timeseries to a forma
  value, for all first values and on up the sequence. Series must be
  supplied as specified by the arguments for `forma-value`. For
  example:

    (forma-seq fire-series short-series long-series t-stat-series)"
  [& in-series]
  [(->> in-series
        (map #(or % (repeat %)))
        (apply map forma-value))])
