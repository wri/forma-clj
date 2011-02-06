;; The idea here is to figure out a way to unpack the rain data, and
;; efficiently get it into a sequence file for later analysis. I
;; should note that the 2.5 degree data is available as an OpenCDF
;; file, with lots of Java support. Might be worthwhile to code that
;; up as a separate input, just in case. (Support for OpenCDF files
;; would sure be nice.)  The other issue we have in this body of code
;; is a lack of with-open. As I'm doing IO with lazy seqs, I simply
;; have to trust that those lazy seqs are going to bottom out, as the
;; end of the lazy seq calls close. As long as they're realized, we're
;; good to go.

;; Java reads its byte arrays in using big endian format -- this rain
;; data was written in little endian format. The way to get these
;; numbers in is to swap them around. In the following multimethod, we
;; allow a number of ways for a little-endian binary file to be
;; accessed using a HeapByteBuffer.

(ns forma.rain
  (:use cascalog.api
        (forma sinu
               [hadoop :only (get-bytes)]))
  (:require [clojure.contrib.io :as io])
  (:import [java.io File InputStream]
           [java.util.zip GZIPInputStream]
           [forma LittleEndianDataInputStream]))

(set! *warn-on-reflection* true)

(def float-bytes (/ ^Integer (Float/SIZE)
                    ^Integer (Byte/SIZE)))

(def map-dimensions [180 360])
(def forma-res 0.5)

(defn dimensions-at-res
  "returns the pixel dimensions at the specified pixel width (in degrees)."
  [res]
  (map #(int (/ % res)) map-dimensions))

(defn area-at-res
  "Area of pixel grid at supplied resolution."
  [res]
  (apply * (dimensions-at-res res)))

(defn floats-for-res
  "Length of the row of floats (in # of bytes) representing the earth
  at the specified resolution."
  [res]
  (* (area-at-res res)
     float-bytes))

;; ## Buffer Slurping

(defn input-stream
  "Attempts to coerce the given argument to an InputStream, with added
  support for gzipped files."
  [arg]
  (let [^InputStream stream (io/input-stream arg)
        rainbuf-size (* 24 (floats-for-res forma-res))]
    (try
      (.mark stream 0)
      (GZIPInputStream. stream rainbuf-size)
      (catch java.io.IOException e
        (.reset stream)
        stream))))

(defmulti little-stream
  "Converts argument into a DataInputStream, with little endian byte
    order. Argument may be an Input Stream, File, String, byte array,
    or another LittleEndianDataInputStream. If the latter is true,
    acts as identity."
  type)

(derive java.lang.String ::fileable)
(derive java.io.File ::fileable)

(defmethod little-stream LittleEndianDataInputStream [x] x)

(defmethod little-stream InputStream [x]
  (LittleEndianDataInputStream. x))

(defmethod little-stream ::fileable [x]
  (little-stream (input-stream (io/file x))))

(defmethod little-stream io/*byte-array-type* [b]
  (little-stream (input-stream b)))

;; ## Data Extraction

;; (Note that none of the various matrix transformations required by
;; FORMA have been done to the data, yet.)

(defn lazy-floats
  "Generates a lazy seq of floats from the supplied
  DataInputStream. Floats are read in little-endian format."
  [^LittleEndianDataInputStream buf]
  (lazy-seq
   (try
     (cons (.readFloat buf) (lazy-floats buf))
     (catch java.io.IOException e
       (.close buf)))))

(defn force-fill
  "Forces the given stream to fill the supplied buffer. In certain
  cases, such as when a GZIPInputStream doesn't have a large enough
  buffer by default, the stream simply won't load the requested number
  of bytes. We keep trying until the damned thing is full."
  [^InputStream stream buffer]
  (loop [len (count buffer)
         off 0]
    (let [read (.read stream buffer off len)
          newlen (- len read)
          newoff (+ off read)]
      (cond (neg? read) read
            (zero? newlen) (count buffer)
            :else (recur newlen newoff)))))

(defn lazy-months
  "Generates a lazy seq of byte-arrays from a binary NOAA PREC/L
  file. Elements alternate between precipitation rate in mm / day and
  total # of gauges."
  ([stream] (lazy-months stream forma-res))
  ([^InputStream stream res]
     (let [arr-size (floats-for-res res)
           buf (byte-array arr-size)]
       (if (pos? (force-fill stream buf))
         (lazy-seq
          (cons buf (lazy-months stream)))
         (.close stream)))))

(defn make-tuple
  "Generates a 3-tuple for NOAA PREC/L months. We increment the index,
  in this case, to make the number correspond to a month of the year,
  rather than an index in a seq."
  [index month]
  (vector "precl" (inc index) (little-stream month)))

(defn rain-tuples
  "Returns a lazy seq of 3-tuples representing NOAA PREC/L rain
  data. Note that we take every other element in the lazy-months seq,
  skipping data concerning # of gauges."
  [stream]
  (map-indexed make-tuple (take-nth 2 (lazy-months stream))))

;; ## Cascalog Queries

(defmapcatop unpack-rain
  ^{:doc "Unpacks a PREC/L binary file for a given year, and returns a
lazy sequence of 3-tuples, in the form of (dataset, month,
data). Assumes that binary files are packaged as hadoop BytesWritable
objects."}
  [stream]
  (let [bytes (get-bytes stream)]
    (rain-tuples (input-stream bytes))))

(defn sqr
  "Returns the square of x."
  [x] (* x x))

(defmapcatop [rain-chunks [chunk-size]]
  ^{:doc "Takes in data for a single month of rain data, and converts
  it into chunks for use in FORMA."}
  [data res]
  (let [edge-length (pixels-at-res (keyword res))]
    (for [xtile (range x-tiles)
          ytile (range y-tiles)
          chunk (range (/ (sqr edge-length) chunk-size))]
      [xtile ytile chunk])))