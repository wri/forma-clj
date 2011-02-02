;; The idea here is to figure out a way to unpack the rain data, and
;; efficiently get it into a sequence file for later analysis. I
;; should note that the 2.5 degree data is available as an OpenCDF
;; file, with lots of Java support. Might be worthwhile to code that
;; up as a separate input, just in case. (Support for OpenCDF files
;; would sure be nice.)

(ns forma.rain
  (:use cascalog.api
        forma.hadoop
        [clojure.contrib.io :only [as-file to-byte-array *byte-array-type*]])
  (:import [java.io File]
           [java.nio FloatBuffer HeapByteBuffer]))

(set! *warn-on-reflection* true)

(def test-file "/Users/sritchie/Desktop/FORMA/RainTest/precl_mon_v1.0.lnx.2009.gri0.5m")

(def *float-bytes* (/ ^Integer (Float/SIZE)
                      ^Integer (Byte/SIZE)))

(def *lat-spread* 180)
(def *lon-spread* 360)
(def *forma-res* 0.5)

(defn dimensions-at-res
  "returns the pixel dimensions at the specified pixel width (in degrees)."
  [res]
  (->> [*lat-spread* *lon-spread*]
       (map #(/ % res))
       (map int)))

(defn area-at-res
  "Area of pixel grid at supplied resolution."
  [res]
  (apply * (dimensions-at-res res)))

(defn floats-for-res
  "Length of the row of floats representing the earth at the specified resolution."
  [res]
  (* (area-at-res res)
     *float-bytes*))

(defn get-float
  "Retrieves the float for the supplied array index, taking
   the byte length of a float into account."
  [index ^HeapByteBuffer array]
  (.getFloat array (* index *float-bytes*)))

;; ## Buffer Slurping

;;Java reads its byte arrays in using big endian format -- this rain
;;data was written in little endian format. The way to get these
;;numbers in is to swap them around. In the following multimethod, we
;;allow a number of ways for a little-endian binary file to be
;;accessed using a HeapByteBuffer.

(defmulti
  #^{:doc "Converts argument into a HeapByteBuffer.  Argument may be
  a File, String, or another HeapByteBuffer.  If the argument is already
  a HeapByteBuffer, returns it."
     :arglists '([arg])}
  to-byte-buffer type)

(defmethod to-byte-buffer HeapByteBuffer [^HeapByteBuffer x] x)

(defmethod to-byte-buffer String [^String str]
  (to-byte-buffer (as-file str)))

(defmethod to-byte-buffer File [^File f]
  (to-byte-buffer (to-byte-array f)))

(defmethod to-byte-buffer *byte-array-type* [^bytes b]
  (-> b
      java.nio.ByteBuffer/wrap
      (.order java.nio.ByteOrder/LITTLE_ENDIAN)))

;; ## Data Extraction

;; The following methods pull various float chunks out of a yearly
;; PREC/L file and return them as a lazy sequence. Note that none of
;; the various matrix transformations required by FORMA have been done
;; to the data, yet.

(defn lazy-floats
  "Creates a lazy seq from the supplied byte buffer."
  [#^HeapByteBuffer buf]
  (lazy-seq
   (try
     (cons (.getFloat buf) (lazy-floats buf))
     (catch java.nio.BufferUnderflowException e
       (.rewind buf)))))

(defn extract-month
  "Extracts the bytes representing rainfall data for a given month from a yearly
   binary PREC/L file. Assumes 0.5 degree resolution."
  [index ^HeapByteBuffer f]
  (let [arr-size (floats-for-res *forma-res*)
        month-offset (* index arr-size 2)
        ret-array (byte-array arr-size)]
    (do (.position f month-offset)
        (.get f ret-array 0 arr-size)
        (to-byte-buffer ret-array))))

(defn all-months
  "Returns a lazy seq of all months inside of a yearly rain array."
  [^HeapByteBuffer buf]
  (let [area (area-at-res *forma-res*)]
    (map-indexed #(vector %1 (take area (lazy-floats %2)))
                 (for [i (range 12)] (extract-month i buf)))))

;; ## Cascalog Queries

(defmapcatop
  #^{:doc "Unpacks a yearly PREC/L binary file, and returns its
months as a lazy sequence."}
  rain-months
  [^BytesWritable stream]
  (let [bytes (get-bytes stream)]
    (all-months (to-byte-buffer bytes))))