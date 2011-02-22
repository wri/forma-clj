;; this namespace provides functions for unpacking binary files of
;; NOAA PRECL data at 0.5 degree resolution, and processing them into
;; tuples suitable for machine learning against MODIS data.
;;
;; As in `forma.hdf`, the overall goal here is to process an NOAA
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
           [java.util.zip GZIPInputStream]))

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
;; using [big endian](http://goo.gl/os4SJ) format, by default. The
;; PRECL dataset was stored using [little endian](http://goo.gl/KUpiy)
;; floats, 4 bytes each. We define a function below that coerces
;; groups of 4 bytes into a float.

(defn little-float
  "Converts four input bits to a float, in little endian
  format. Special case of a more general `littleize` function, on the
  horizon."
  [b0 b1 b2 b3]
  (Float/intBitsToFloat
   (bit-or
   (bit-shift-left b3 24)
   (bit-or
    (bit-shift-left (bit-and b2 0xff) 16)
    (bit-or
     (bit-shift-left (bit-and b1 0xff) 8)
     (bit-and b0 0xff))))))

(defn little-floats
  "Converts the supplied byte array into a seq of
  little-endian-ordered floats."
  [bytes]
  (vec (map (partial apply little-float)
            (partition float-bytes bytes))))

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
  (vector "precl" (inc index) (little-floats month)))

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

;; Each PRECL filename contains the year from which the data is
;; sourced. We currently decide time period based on spatial
;; resolution -- this will cause problems later, when we start
;; interacting with datasets at the same spatial resolution and
;; different temporal resolution. A rework of `datetime->period` will
;; fix this.

(defmapop [extract-period [m-res]]
  ^{:doc "Extracts the year from a NOAA PREC/L filename (assuming that
  the year is the only group of 4 digits), pairs it with the supplied
  month, and returns an integerized time period based on the supplied
  spatial resolution value."}
  [filename month]
  (let [year (Integer/parseInt (first (re-find #"(\d{4})" filename)))]
    [m-res (datetime->period m-res year month)]))

;; The following functions bring everything together, making heavy use
;; of the functionality supplied by `forma.reproject`. Rather than
;; sampling PRECL data at the positions of every possible MODIS tile,
;; we allow the user to supply a seq of TileIDs for processing. (No
;; MODIS product covers every possible tile. The terra products only
;; cover land, for example.)
;;
;; These functions are designed to be able to map between WGS84 and
;; MODIS datasets at arbitrary resolution. This explains the
;; resolution parameters seen in the functions below.

(defn index-seqs
  "Cascalog subquery to generate translation indices for chunks within
  each tile referenced in the supplied sequence of MODIS
  TileIDs. Requires MODIS and WGS84 spatial resolutions, and a chunk
  size."
  [m-res ll-res c-size tile-seq]
  (let [source (memory-source-tap tile-seq)]
    (<- [?mod-h ?mod-v ?chunkid ?idx-seq]
        (source ?mod-h ?mod-v)
        (chunk-samples [m-res ll-res c-size]
                       ?mod-h ?mod-v :> ?chunkid ?idx-seq))))

(defn rain-months
  "Cascalog subquery to extract all months from a directory of PREC/L
  datasets at the supplied WGS84 spatial resolution, paired with time
  periods appropriate for the supplied MODIS spatial
  resolution. Source can be any tap that supplies files like
  precl_mon_v1.0.lnx.2010.gri0.5m(.gz, optionally)."
  [m-res ll-res source]
  (<- [?dataset ?m-res ?period ?raindata]
      (source ?filename ?file)
      (unpack-rain [ll-res] ?file :> ?dataset ?month ?raindata)
      (extract-period [m-res] ?filename ?month :> ?m-res ?period)))

(defmapop fancyindex [coll indices]
  ^{:doc "Samples the supplied collection, returning the values at the
  supplied indices within coll. Analogous to NumPy's [fancy
  indexing](http://goo.gl/rZ4ri)."}
  [(map (vec coll) indices)])

;; And, finally, the chunker. This subquery is analogous to the
;; chunker found in `forma.hdf`. This is the only function that needs
;; to be called, when processing new PRECL data.

(defn rain-chunks
  "Cascalog subquery to fully process a WGS84 float array at the
  supplied resolution (`ll-res`) into tuples suitable for comparison
  to any MODIS dataset at the supplied modis resolution (`m-res`),
  partitioned by the supplied chunk size."
  [m-res ll-res c-size tile-seq source]
  (let [indices (index-seqs m-res ll-res c-size tile-seq)
        precl (rain-months m-res ll-res source)]
    (<- [?dataset ?res ?mod-h ?mod-v ?period ?chunkid ?chunk]
        (indices ?mod-h ?mod-v ?chunkid ?idx-seq)
        (precl ?dataset ?res ?period ?raindata)
        (identity 1 :> ?_)        
        (fancyindex ?raindata ?idx-seq :> ?chunk))))