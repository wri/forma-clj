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

(ns forma.rain
  (:use (cascalog [api :exclude (union)])
        (clojure [set :only (union)])
        (forma sinu
               [hadoop :only (get-bytes)]))
  (:require (clojure.contrib [io :as io]))
  (:import [java.io File InputStream]
           [java.util.zip GZIPInputStream]
           [forma LittleEndianDataInputStream]))

(set! *warn-on-reflection* true)

(def map-dimensions [180 360])
(def forma-res 0.5)

(defn sqr
  "Returns the square of x."
  [x] (* x x))

(defn dimensions-at-res
  "returns the pixel dimensions at the specified pixel width (in degrees)."
  [res]
  (map #(int (/ % res)) map-dimensions))

(defn area-at-res
  "Area of pixel grid at supplied resolution."
  [res]
  (apply * (dimensions-at-res res)))

(def float-bytes (/ ^Integer (Float/SIZE)
                    ^Integer (Byte/SIZE)))

(defn floats-for-res
  "Length of the row of floats (in # of bytes) representing the earth
  at the specified resolution."
  [res]
  (* (area-at-res res) float-bytes))

;; ## Buffer Slurping

(defn input-stream
  "Attempts to coerce the given argument to an InputStream, with added
  support for gzipped files. If the input argument does point to a
  gzipped file, the default buffer will be sized to fit one NOAA
  PREC/L binary data file at 0.5 degree resolution, or 24 groups of (*
  360 720) floats. To make sure that the returned GZIPInputStream will
  fully read into a supplied byte array, see forma.rain/force-fill."
  [arg]
  (let [^InputStream stream (io/input-stream arg)
        rainbuf-size (* 24 (floats-for-res forma-res))]
    (try
      (.mark stream 0)
      (GZIPInputStream. stream rainbuf-size)
      (catch java.io.IOException e
        (.reset stream)
        stream))))

;; Java reads its byte arrays in using big endian format -- this rain
;; data was written in little endian format. The way to get these
;; numbers in is to swap them around. Here, we provide a method for a
;; little-endian binary file to be accessed using its own custom DataInputStream.

(defn little-stream
  "Returns a little endian DataInputStream opened up on the supplied
  argument. See clojure.contrib.io/input-stream for acceptable
  argument types."
  [x]
  (LittleEndianDataInputStream. (input-stream x)))

;; ## Data Extraction

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

(defmapcatop
  ^{:doc "Unpacks a PREC/L binary file for a given year, and returns a
lazy sequence of 3-tuples, in the form of (dataset, month,
data). Assumes that binary files are packaged as hadoop BytesWritable
objects."}
  unpack-rain [stream]
  (let [bytes (get-bytes stream)]
    (rain-tuples (input-stream bytes))))

;; ## Resampling of Rain Data
;; I'm going to stop in the middle of this, as I know I'm not doing a
;; good job -- but we're almost done, here. The general idea is to
;; take in a rain data month, and generate a list comprehension with
;; every possible chunk at the current resolution. This will stream
;; out a lazy seq of 4 tuples, containing rain data for a given MODIS
;; chunk, only for valid MODIS tiles. If any functions don't make
;; sense, look at sinu.clj. This matches up with stuff over there.

;; (Note that none of the various matrix transformations required by
;; FORMA have been done to the data, yet. We need to account for the
;; horizontal swap and vertical flip, when we do our index lookup.)

(defn valid-modis?
  "Checks a MODIS tile coordinate against the set of all MODIS tiles
  with some form of valid data within them. See
  http://remotesensing.unh.edu/modis/modis.shtml for a clear picture
  of which tiles are considered valid."
  [h v]
  (contains? good-tiles [h v]))

(defn tile-position
  "For a given MODIS chunk and index within that chunk, returns
  [sample, line] within the MODIS tile."
  [chunk index chunk-size]
  (let [line (* chunk chunk-size)
        sample (+ line index)]
    [sample line]))

(defn rain-index
  "INCOMPLETE. Returns the index inside a rain data vector for the
  given inputs."
  [mod-h mod-v sample line res]
  (geo-coords mod-h mod-v sample line res))

(defn resample
  "Takes in a month's worth of PREC/L rain data, and returns a lazy
  seq of data samples for supplied MODIS chunk coordinates."
  [data res mod-h mod-v chunk chunk-size]
  (let [rain (vec data)]
    (for [pixel (range chunk-size)]
      (let [[sample line] (tile-position chunk pixel chunk-size)
            index (rain-index mod-h mod-v sample line res)]
        (rain index)))))

(defmapcatop [rain-chunks [chunk-size]]
  ^{:doc "Takes in data for a single month of rain data, and resamples
  it to the MODIS sinusoidal grid at the supplied resolution. Returns
  4-tuples, looking like (mod-h, mod-v, chunk, chunkdata-seq)."}
  [data res]
  (let [edge-length (pixels-at-res res)]
    (for [mod-h (range h-tiles)
          mod-v (range v-tiles)
          chunk (range (/ (sqr edge-length) chunk-size))
          :when (valid-modis? mod-h mod-v)]
      (let [chunk-seq (resample data res mod-h mod-v chunk chunk-size)]
        [mod-h mod-v chunk chunk-seq]))))