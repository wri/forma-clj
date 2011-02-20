;; TODO -- what's the point of this file? As it is now, we're
;; unpacking the rain data, and that's it! We take a binary, little
;; endian PRECL dataset and we unpack it into chunks for use in our
;; algorithms. Why does it take so much effort?

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
  (:use cascalog.api
        (forma [hadoop :only (get-bytes)]
               [reproject :only (chunk-samples)]
               [conversion :only (datetime->period)]))
  (:require (clojure.contrib [io :as io]))
  (:import [java.io File InputStream]
           [java.util.zip GZIPInputStream]
           [forma LittleEndianDataInputStream]))

(set! *warn-on-reflection* true)

(def map-dimensions [360 180])

(defn dimensions-at-res
  "returns the pixel dimensions at the specified pixel width (in degrees)."
  [ll-res]
  (map #(quot % ll-res) map-dimensions))

(defn area-at-res
  "Area of pixel grid at supplied resolution."
  [ll-res]
  (apply * (dimensions-at-res ll-res)))

(def float-bytes (/ ^Integer (Float/SIZE)
                    ^Integer (Byte/SIZE)))

(defn floats-for-res
  "Length of the row of floats (in # of bytes) representing the earth
  at the specified resolution."
  [res]
  (* (area-at-res res) float-bytes))

;; ## Buffer Slurping

;; TODO -- get the fixed numbers out of here! 0.5, 25
(defn input-stream
  "Attempts to coerce the given argument to an InputStream, with added
  support for gzipped files. If the input argument does point to a
  gzipped file, the default buffer will be sized to fit one NOAA
  PREC/L binary data file at 0.5 degree resolution, or 24 groups of (*
  360 720) floats. To make sure that the returned GZIPInputStream will
  fully read into a supplied byte array, see forma.rain/force-fill."
  [arg]
  (let [^InputStream stream (io/input-stream arg)
        rainbuf-size (* 24 (floats-for-res 0.5))]
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
  ([ll-res ^InputStream stream]
     (let [arr-size (floats-for-res ll-res)
           buf (byte-array arr-size)]
       (if (pos? (force-fill stream buf))
         (lazy-seq
          (cons buf (lazy-months ll-res stream)))
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
  [ll-res stream]
  (map-indexed make-tuple (take-nth 2 (lazy-months ll-res stream))))

;; ## Cascalog Queries

(defmapcatop [unpack-rain [ll-res]]
  ^{:doc "Unpacks a PREC/L binary file for a given year, and returns a
lazy sequence of 3-tuples, in the form of (dataset, month,
data). Assumes that binary files are packaged as hadoop BytesWritable
objects."}
  [stream]
  (let [bytes (get-bytes stream)]
    (rain-tuples ll-res (input-stream bytes))))

;; TODO -- update this. We need m-res because we need to act properly
;; on the datetime->period function, which currently depends on
;; resolution. We should probably change this to work on the time
;; period, like sixteens, days, etc.
(defmapop [extract-period [m-res]]
  ^{:doc "Extracts the year from a NOAA PREC/L filename, assuming that
  the year is the only group of 4 digits. pairs it with the supplied
  month, and converts both into a time period, with units in months
  from the reference start date as defined in conversion.clj."}
  [filename month]
  (let [year (Integer/parseInt (first (re-find #"(\d{4})" filename)))]
    [m-res (datetime->period m-res year month)]))

(defn index-seqs
  "Takes a tile sequence and a couple of resolution parameters --
  returns translation indices for chunks within a tile, given the chunk size."
  [m-res ll-res c-size tile-seq]
  (let [source (memory-source-tap tile-seq)]
    (<- [?mod-h ?mod-v ?chunkid ?idx-seq]
        (source ?mod-h ?mod-v)
        (chunk-samples [m-res ll-res c-size]
                       ?mod-h ?mod-v :> ?chunkid ?idx-seq))))

;; TODO -- explain in docstring why ll-res, etc is important!
(defn rain-months
  "Returns all months from a directory of PREC/L datasets at the
  supplied resolution, paired with their time periods."
  [m-res ll-res source]
  (<- [?dataset ?m-res ?period ?raindata]
        (source ?filename ?file)
        (unpack-rain [ll-res] ?file :> ?dataset ?month ?raindata)
        (extract-period [m-res] ?filename ?month :> ?m-res ?period)))

;; TODO -- update documentation
(defmapop fancyindex [f coll]
  [(apply vector (map (vec f) coll))])

;; TODO -- update documentation
(defn rain-chunks
  "Generates rain chunks for the given tiles, at the supplied
  resolutions on each end of the chain."
  [m-res ll-res c-size tile-seq source]
  (let [indices (index-seqs m-res ll-res c-size tile-seq)
        precl (rain-months m-res ll-res source)]
    (<- [?dataset ?res ?mod-h ?mod-v ?period ?chunkid ?chunk]
        (indices ?mod-h ?mod-v ?chunkid ?idx-seq)
        (precl ?dataset ?res ?period ?raindata)
        (identity 1 :> ?_)        
        (fancyindex ?raindata ?idx-seq :> ?chunk))))