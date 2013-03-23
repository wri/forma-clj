(ns forma.thrift
  "This namespace provides an API for creating, accessing fields of, and
   unpacking Thrift objects defined in the dev/forma.thrift IDL. 

  The following functions are available for creating Thrift objects:

    NeighborValue*
    FireValue*
    ModisChunkLocation*
    ModisPixelLocation*
    TimeSeries*
    FormaValue*
    DataChunk*

  Example usage for creating a TimeSeries:

    > (TimeSeries* 0 1 [1.0 2.0 3.0])
    #<TimeSeries TimeSeries(
        startIdx:0,
        endIdx:1,
        series:<ArrayValue doubles:DoubleArray(doubles:[1.0, 2.0, 3.0])>)>

  To unpack Thrift objects, use the unpack function which returns a vector of
  Thrift object field values in the order defined in the IDL.

  Example usage for unpacking a TimeSeries:

    > (def ts (TimeSeries* 0 1 [1.0 2.0 3.0]))
    > (unpack ts)
    [0 1 #<ArrayValue <ArrayValue doubles:DoubleArray(doubles:[1.0, 2.0, 3.0])>>]

  Example usage for unpacking the TimeSeries series field:

    > (unpack (get-series ts)
    [1.0 2.0 3.0]"
  (:import [forma.schema
            ArrayValue DataChunk DataValue DoubleArray FireArray
            FireValue FormaValue IntArray LocationProperty
            LocationPropertyValue LongArray ModisChunkLocation
            ModisPixelLocation ShortArray TimeSeries FormaArray
            NeighborValue Pedigree]
           [java.util ArrayList]
           [org.apache.thrift TBase TUnion]))

(defn epoch  
  []
  "Return seconds since epoch."
  (int (/ (System/currentTimeMillis) 1000)))

;; Protocols for accessing Thrift object fields:
(defprotocol ITUnion
  (get-field-value [x]))

(defprotocol IDataChunk
  (get-dataset [x])
  (get-location-property [x])
  (get-chunk-value [x])
  (get-temporal-res [x])
  (get-date [x]))

(defprotocol ITimeSeries
  (get-start-idx [x])
  (get-end-idx [x])
  (get-series [x]))

(defprotocol ILocationProperty
  (get-property [x]))

(defprotocol IModisChunkLocation
  (get-chunk-resolution [x])
  (get-chunk-tile-h [x])
  (get-chunk-tile-v [x])
  (get-chunk-id [x])
  (get-chunk-size [x]))

(defprotocol IModisPixelLocation
  (get-pixel-resolution [x])
  (get-pixel-tile-h [x])
  (get-pixel-tile-v [x])
  (get-sample [x])
  (get-line [x]))

(defprotocol IFireValue
  (get-temp-330 [x])
  (get-conf50 [x])
  (get-both-preds [x])
  (get-count [x]))

(defprotocol IFormaValue
  (get-forma-fire-value [x])
  (get-short-drop [x])
  (get-long-drop [x])
  (get-tstat [x])
  (get-param-break [x]))

(defprotocol INeighborValue
  (get-fire-value [x])
  (get-num-neighbors [x])
  (get-avg-short-drop [x])
  (get-min-short-drop [x])
  (get-avg-long-drop [x])
  (get-min-long-drop [x])
  (get-avg-tstat [x])
  (get-min-tstat [x])
  (get-avg-param-break [x])
  (get-min-param-break [x]))

;; Protocol for packing an Clojure data structure into a Thrift object.
(defprotocol IPackable
  (pack [x]))

;; Protocol for unpacking a Thrift object into a Clojure data structure.
(defprotocol IUnpackable
  (unpack [x]))

;; Multimethods for wrapping arrays in ArrayValue:
(defmulti mk-array-value class)
(defmethod mk-array-value IntArray [x] (ArrayValue/ints x))
(defmethod mk-array-value DoubleArray [x] (ArrayValue/doubles x))
(defmethod mk-array-value LongArray [x] (ArrayValue/longs x))
(defmethod mk-array-value ShortArray [x] (ArrayValue/shorts x))
(defmethod mk-array-value FireArray [x] (ArrayValue/fires x))
(defmethod mk-array-value FormaArray [x] (ArrayValue/formas x))

;; Multimethods for wrapping objects in DataValue:
(defmulti mk-data-value class)
(defmethod mk-data-value Double [x] (DataValue/doubleVal x))
(defmethod mk-data-value Integer [x] (DataValue/intVal x))
(defmethod mk-data-value Long [x] (DataValue/longVal x))
(defmethod mk-data-value Short [x] (DataValue/shortVal x))
(defmethod mk-data-value FireValue [x] (DataValue/fireVal x))
(defmethod mk-data-value TimeSeries [x] (DataValue/timeSeries x))
(defmethod mk-data-value ArrayValue [x] (DataValue/vals x))
(defmethod mk-data-value FormaValue [x] (DataValue/forma x))

;; Multimethods for wrapping location objects in LocationProperty:
(defmulti mk-location-prop class)
(defmethod mk-location-prop ModisPixelLocation [x]
  (->> x LocationPropertyValue/pixelLocation LocationProperty.))
(defmethod mk-location-prop ModisChunkLocation [x]
  (->> x LocationPropertyValue/chunkLocation LocationProperty.))

(defn list-of
  "Return a java.util.ArrayList that contains the result of mapping a function
  across all entries in the supplied sequence."
  [f xs]
  (ArrayList. (for [x xs]
                (try (f x)
                     (catch Exception e nil)))))

(defn int-struct
  "Return a IntArray that contains all numbers in the supplied sequence cast
  to ints."
  [xs]
  (let [ints (map int xs)]
    (doto (IntArray.)
      (.setInts ints))))

(defn double-struct
  "Return a DoubleArray that contains all numbers in the supplied sequence cast
   to doubles."
  [xs]
  (let [doubles (list-of double xs)]
    (doto (DoubleArray.)
      (.setDoubles doubles))))

(defn forma-array
  "Return a FormaArray object created from the supplied sequence of FormaValue
   objects."
  [xs]
  (doto (FormaArray.)
    (.setValues (ArrayList. xs))))

(defn fire-array
  "Return a FireArray object created from the supplied sequence of FireValue
   objects."
  [xs]
  (doto (FireArray.)
    (.setFires (ArrayList. xs))))

(defn DataValue?
  "Return true if x is a supported DataValue type, otherwise nil."
  [x]
  (let [types [forma.schema.DoubleArray forma.schema.FireArray
               forma.schema.FireValue forma.schema.FormaArray
               forma.schema.FormaValue forma.schema.IntArray
               forma.schema.LongArray forma.schema.TimeSeries
               java.lang.Double java.lang.Integer java.lang.Long
               java.lang.Short]
        vals (if (coll? x) x (vector x))]
    (some (fn [val] (some #(= (type val) %) types)) vals)))

(defn TimeSeries?
  "Return true if x is a supported TimeSeries value type, otherwise nil."
  [x]   
  (if (not (coll? x)) false 
      (let [types [forma.schema.DoubleArray forma.schema.FireArray
                   forma.schema.FireValue forma.schema.FormaArray
                   forma.schema.FormaValue forma.schema.IntArray
                   forma.schema.LongArray java.lang.Double
                   java.lang.Integer java.lang.Long]
            vals (if (coll? x) x (vector x))]
        (some (fn [val] (some #(= (type val) %) types)) vals))))

(defn LocationPropertyValue?
  "Return true if x is a LocationPropertyValue type, otherwise nil."
  [x]
  (or (= (type x) forma.schema.ModisChunkLocation)
      (= (type x) forma.schema.ModisPixelLocation)))

(defn NeighborValue*
  "Create a NeighborValue."
  [fire ncount avg-short min-short avg-long min-long avg-stat min-stat
   & [avg-break min-break]]
  {:pre [(instance? forma.schema.FireValue fire)
         (or (nil? avg-break) (float? avg-break))
         (or (nil? min-break) (float? min-break))
         (integer? ncount)
         (every? float?
                 [avg-short min-short avg-long min-long avg-stat min-stat])]}
  (let [n-value (NeighborValue. fire ncount avg-short min-short avg-long min-long
                                avg-stat min-stat)]
    (if avg-break
      (doto n-value
        (.setAvgParamBreak avg-break)))
    (if min-break
      (doto n-value
        (.setMinParamBreak min-break)))
    n-value))

(defn FireValue*
  "Create a FireValue."
  [temp-330 conf-50 both-preds count]
  {:pre [(every? #(>= % 0) [temp-330 conf-50 both-preds count])]}
  (FireValue. temp-330 conf-50 both-preds count))

(defn ModisChunkLocation*
  "Create a ModisChunkLocation."
  [s-res h v id size]
  {:pre [(string? s-res)
         (every? integer? [h v id size])]}
  (ModisChunkLocation. s-res h v id size))

(defn ModisPixelLocation*
  "Create a ModisPixelLocation."
  [s-res h v sample line]
  {:pre [(string? s-res)
         (every? integer? [h v sample line])]}
  (ModisPixelLocation. s-res h v sample line))

(defn TimeSeries*
  "Create a TimeSeries."
  ([start vals]
    {:pre [(every? integer? [start])
           (or (instance? IntArray vals)
               (instance? DoubleArray vals)
               (instance? LongArray vals)
               (instance? ShortArray vals)
               (instance? FireArray vals)
               (instance? FormaArray vals)
               (coll? vals))]}
    (let [elems (if (coll? vals)
                  (count vals)
                  (count (vec (unpack vals))))]
      (TimeSeries* start
                   (dec (+ start elems))
                   vals)))
  ([start end vals]
    {:pre [(every? integer? [start end])
           (or (instance? IntArray vals)
               (instance? DoubleArray vals)
               (instance? LongArray vals)
               (instance? ShortArray vals)
               (instance? FireArray vals)
               (instance? FormaArray vals)
               (coll? vals))]}
    (let [series (if (coll? vals) (pack vals) vals)]
      (TimeSeries. start end (mk-array-value series)))))

(defn FormaValue*
  "Create a FormaValue."
  [fire short long tstat & break]
  {:pre [(instance? forma.schema.FireValue fire)
         (every? float? [short long tstat])
         (or (not break) (float? (first break)))]}
  (let [[break] break
        forma-value (FormaValue. fire short long tstat)]
    (if break
      (doto forma-value
        (.setParamBreak break)))
    forma-value))

(defn DataChunk*
  "Create a DataChunk."
  [name loc val res & {:keys [date pedigree] :or {date nil pedigree (epoch)}}]
  {:pre  [(every? string? [name res])
          (or (nil? date) (string? date))
          (or (nil? pedigree) (integer? pedigree))
          (LocationPropertyValue? loc)
          (DataValue? val)]}
  (let [loc (mk-location-prop loc)
        val (if (coll? val)
              (->> val pack mk-array-value mk-data-value)
              (mk-data-value val))
        chunk (DataChunk. name loc val res)]
    (if date
      (doto chunk
        (.setDate date)))
    (if pedigree
      (doto chunk
        (.setPedigree (Pedigree. pedigree))))
    chunk))

(extend-protocol ITUnion
  TUnion
  (get-field-value [x] (.getFieldValue x)))

(extend-protocol ILocationProperty
  LocationProperty
  (get-property [x] (.getProperty x)))

(extend-protocol IModisPixelLocation
  ModisPixelLocation
  (get-pixel-resolution [x] (.getResolution x))
  (get-pixel-tile-h [x] (.getTileH x))
  (get-pixel-tile-v [x] (.getTileV x))  
  (get-sample [x] (.getSample x))
  (get-line [x] (.getLine x)))  

(extend-protocol IModisChunkLocation
  ModisPixelLocation
  (get-chunk-resolution [x] (.getResolution x))
  (get-chunk-tile-h [x] (.getTileH x))
  (get-chunk-tile-v [x] (.getTileV x))  
  (get-chunk-id [x] (.getChunkID x))
  (get-chunk-size [x] (.getChunkSize x)))  

(extend-protocol ITimeSeries
  TimeSeries
  (get-start-idx [x] (.getStartIdx x))
  (get-end-idx [x] (.getEndIdx x))
  (get-series [x] (.getSeries x)))

(extend-protocol IFireValue
  FireValue
  (get-temp-330 [x] (.getTemp330 x))
  (get-conf50 [x] (.getConf50 x))
  (get-both-preds [x] (.getBothPreds x))
  (get-count [x] (.getCount x)))

(extend-protocol IFormaValue
  FormaValue
  (get-forma-fire-value [x] (.getFireValue x))
  (get-short-drop [x] (.getShortDrop x))
  (get-long-drop [x] (.getLongDrop x))
  (get-tstat [x] (.getTStat x))
  (get-param-break [x] (.getParamBreak x)))

(extend-protocol INeighborValue
  NeighborValue
  (get-fire-value [x] (.getFireValue x))
  (get-num-neighbors [x] (.getNumNeighbors x))
  (get-avg-short-drop [x] (.getAvgShortDrop x))
  (get-min-short-drop [x] (.getMinShortDrop x))
  (get-avg-long-drop [x] (.getAvgLongDrop x))
  (get-min-long-drop [x] (.getMinLongDrop x))
  (get-avg-tstat [x] (.getAvgTStat x))
  (get-min-tstat [x] (.getMinTStat x))
  (get-avg-param-break [x] (.getAvgParamBreak x))
  (get-min-param-break [x] (.getMinParamBreak x)))

(extend-protocol IPackable
  java.lang.Iterable
  (pack [[v :as xs]]
    (cond (= forma.schema.FormaValue (type v)) (forma-array xs)
          (= forma.schema.FireValue (type v)) (fire-array xs)
          (integer? v) (int-struct xs)
          (float? v) (double-struct xs))))

(extend-protocol ITimeSeries
  TimeSeries
  (get-start-idx [x] (.getStartIdx x))
  (get-end-idx [x] (.getEndIdx x))
  (get-series [x] (.getSeries x)))

(extend-protocol IDataChunk  
  DataChunk
  (get-dataset [x] (.getDataset x))
  (get-location-property [x] (.getLocationProperty x))
  (get-chunk-value [x] (.getChunkValue x))
  (get-temporal-res [x] (.getTemporalRes x))
  (get-date [x] (.getDate x)))

(extend-protocol IUnpackable
  LocationProperty
  (unpack [x] (->> x get-property get-field-value unpack))
  
  ModisPixelLocation
  (unpack [x] (vec (map #(.getFieldValue x %) (keys (ModisPixelLocation/metaDataMap)))))

  ModisChunkLocation
  (unpack [x] (vec (map #(.getFieldValue x %) (keys (ModisChunkLocation/metaDataMap)))))

  FireValue
  (unpack [x] (vec (map #(.getFieldValue x %) (keys (FireValue/metaDataMap)))))
  
  TimeSeries
  (unpack [x] (vec (map #(.getFieldValue x %) (keys (TimeSeries/metaDataMap)))))
  
  FormaArray
  (unpack [x] (->> x .getValues vec))

  ShortArray
  (unpack [x] (->> x .getShorts vec))

  IntArray
  (unpack [x] (->> x .getInts vec))

  LongArray
  (unpack [x] (->> x .getLongs vec))

  DoubleArray
  (unpack [x] (->> x .getDoubles vec))

  DataValue
  (unpack
    [x]
    (let [val (->> x .getFieldValue)]
      (cond (instance? TUnion val) (unpack val)
            (instance? TBase val) (unpack val)
            :else val)))
  
  FormaValue
  (unpack [x] (vec (map #(.getFieldValue x %) (keys (FormaValue/metaDataMap)))))

  NeighborValue
  (unpack [x] (vec (map #(.getFieldValue x %) (keys (NeighborValue/metaDataMap)))))

  FireArray
  (unpack [x] (->> x .getFires vec))

  DataChunk
  (unpack [x] (vec (map #(.getFieldValue x %) (keys (DataChunk/metaDataMap)))))

  ;; DataChunk
  ;; (unpack [x]
  ;;   (let [[name loc data t-res date]
  ;;         (map #(.getFieldValue x %) (keys (DataChunk/metaDataMap)))
  ;;         loc (->> loc get-property get-field-value)
  ;;         data (->> data .getFieldValue)]
  ;;     [name loc data t-res date]))
  
  ArrayValue
  (unpack [x] (->> x .getFieldValue unpack))

  Pedigree
  (unpack [x] (->> x .getTrueAsOfSecs)))

(defn count-vals
  "Return the count of elements in the supplied Tnrift object."
  [x]
  {:pre [(coll? (unpack x))]}
  (->> x unpack count))

(defn unpack*
  "Unpack a Thrift object and return it wrapped in a vector. Common use case is
  calling this within a Cascalog query."
  [x]
  (vector (unpack x)))
