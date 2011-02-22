;; this namespace provides functions for unpacking binary files of
;; NOAA PRECL data at 0.5 degree resolution, and processing them into
;; tuples suitable for machine learning against MODIS data.
;;
;; As with modis.clj, the overall goal here is to process an NOAA
;; PREC/L dataset into tuples of the form
;;
;;     [?dataset ?res ?tileid ?tperiod ?chunkid ?chunk-pix]

(ns forma.rain
  (:use cascalog.api
        (forma [hadoop :only (get-bytes)]
               [reproject :only (chunk-samples dimensions-at-res)]
               [conversion :only (datetime->period)]))
  (:require (clojure.contrib [io :as io]))
  (:import [java.io File InputStream]
           [java.util.zip GZIPInputStream]
           [forma LittleEndianDataInputStream]))

;; ## Dataset Information
;;
;; We chose to go with PRECL for our rain analysis, though any of the
;; global datasets listed in the [IRI/LDEO Climate Data
;; Library's](http://goo.gl/RmTKe) [atmospheric data
;; page](http://goo.gl/V5tUI) would be great.
;;
;; More information on the dataset can be found at the [NOAA PRECL
;; info site] (http://goo.gl/V3gqv). The data are fully documented in
;; [Global Land Precipitation: A 50-yr Monthly Analysis Based on Gauge
;; Observations] (http://goo.gl/PVOr6).
;;
;; ### Data Processing
;;
;; The NOAA PRECL data comes in files of binary arrays, with one file
;; for each year. Each file holds 24 datasets, alternating between
;; precip. rate in mm/day and total # of gauges, gridded in WGS84 at
;; 0.5 degree resolution. No metadata exists to mark byte offsets. We
;;process these datasets using input streams, so we need to know in
;;advance how many bytes to read off per dataset.

(def float-bytes (/ ^Integer (Float/SIZE)
                    ^Integer (Byte/SIZE)))

(defn floats-for-res
  "Length of the row of floats (in # of bytes) representing the earth
  at the specified resolution."
  [res]
  (apply * float-bytes (dimensions-at-res res)))

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

;; ### Data Extraction
;;
;; When using an input-stream to feed lazy-seqs, it becomes difficult
;; to use `with-open`, as we don't know when the application will be
;; finished with the stream. We deal with this by calling close on the
;; stream when our lazy-seq bottoms out.

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

;; Once we have the byte arrays returned by `lazy-months`, we need to
;; process each one into meaningful data. Java reads primitive arrays
;; using [big endian](http://goo.gl/os4SJ) format, be default. The
;; PRECL dataset was stored using [little endian](http://goo.gl/KUpiy)
;; floats. We wrap `(input-stream bytes)` to force all reads to take
;; place using little endian format.

(defn lazy-floats
  "Generates a lazy seq of floats from the supplied byte array. Floats
  are read in little-endian format."
  [bytes]
  (let [buf (LittleEndianDataInputStream.
             (input-stream bytes))]
    (lazy-seq
     (try
       (cons (.readFloat buf) (lazy-floats buf))
       (catch java.io.IOException e
         (.close buf))))))

;; As our goal is to process each file into the tuple-set described
;; above, we need to tag each month with metadata, to distinguish it
;; from other datasets, and standardize the field information for
;; later queries against all data. We hardcode the name "precl" for
;; the data here, and store the month as an index, for later
;; conversion to time period.

(defn make-tuple
  "Generates a 3-tuple for NOAA PREC/L months. We increment the index,
  in this case, to make the number correspond to a month of the year,
  rather than an index in a seq."
  [index month]
  (vector "precl" (inc index) (lazy-floats month)))

(defn rain-tuples
  "Returns a lazy seq of 3-tuples representing NOAA PREC/L rain
  data. Note that we take every other element in the lazy-months seq,
  skipping data concerning # of gauges."
  [ll-res stream]
  (map-indexed make-tuple
               (take-nth 2 (lazy-months ll-res stream))))

;; This is our first cascalog query -- we use `defmapcatop`, as we
;; need to split each file out into twelve separate tuples, one for
;; each month.

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

;; Each PRECL filename contains the year from which the data is
;; sourced.

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