;; The idea here is to figure out a way to unpack the rain data, and
;; efficiently get it into a sequence file for later analysis. I
;; should note that the 2.5 degree data is available as an OpenCDF
;; file, with lots of Java support. Might be worthwhile to code that
;; up as a separate input, just in case. (Support for OpenCDF files
;; would sure be nice.)

(ns forma.rain
  (:use cascalog.api
        forma.hadoop
        [clojure.contrib.io :only [input-stream file *byte-array-type*]])
  (:import [java.io File InputStream]
           [forma LittleEndianDataInputStream]))

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

;; ## Buffer Slurping

;;Java reads its byte arrays in using big endian format -- this rain
;;data was written in little endian format. The way to get these
;;numbers in is to swap them around. In the following multimethod, we
;;allow a number of ways for a little-endian binary file to be
;;accessed using a HeapByteBuffer.

(derive java.lang.String ::fileable)
(derive java.io.File ::fileable)

(defmulti #^{:doc "Converts argument into a DataInputStream, with
    little endian byte order. Argument may be an Input Stream, File,
    String, byte array, or another LittleEndianDataInputStream. If the
    latter is true, acts as identity."
             :arglists '([arg])}
  little-stream type)

(defmethod little-stream LittleEndianDataInputStream [^LittleEndianDataInputStream x] x)

(defmethod little-stream InputStream [^InputStream x]
  (LittleEndianDataInputStream. x))

(defmethod little-stream ::fileable [x]
  (LittleEndianDataInputStream. (input-stream (file x))))

(defmethod little-stream *byte-array-type* [^bytes b]
  (LittleEndianDataInputStream. (input-stream b)))

;; ## Data Extraction

;; (Note that none of the various matrix transformations required by
;; FORMA have been done to the data, yet.)

(defn lazy-floats
  "Creates a lazy seq from the supplied byte buffer."
  [^LittleEndianDataInputStream buf]
  (lazy-seq
   (try
     (cons (.readFloat buf) (lazy-floats buf))
     (catch java.io.IOException e
       (.close buf)))))

(defn lazy-months
  "Generates a lazy seq of byte-arrays from a binary NOAA PREC/L
  file. Elements alternate between precipitation rate in mm / day and
  total # of gauges."
  ([^InputStream stream] (lazy-months stream *forma-res*))
  ([^InputStream stream res]
     (let [arr-size (floats-for-res res)
           buf (byte-array arr-size)]
       (if (pos? (.read stream buf))
         (lazy-seq
          (cons buf (lazy-months stream)))))))

(defn all-months
  "Returns a lazy seq of all months inside of a yearly rain array."
  [^InputStream stream]
    (map-indexed #(vector %1 (lazy-floats (little-stream %2)))
                 (take-nth 2 (lazy-months stream))))

;; ## Cascalog Queries
;; [TODO] Add in with-open, where appropriate.

(defmapcatop
  #^{:doc "Unpacks a yearly PREC/L binary file, and returns its
    months as a lazy sequence."}
  rain-months
  [^BytesWritable stream]
  (let [bytes (get-bytes stream)]
    (all-months (input-stream bytes))))