;; this namespace provides functions for unpacking binary files of
;; NOAA PRECL data at 0.5 degree resolution, and processing them into
;; tuples suitable for machine learning against MODIS data.
;;
;; As in `forma.source.hdf`, the overall goal here is to process an
;; NOAA PREC/L dataset into tuples of the form
;;
;;     [?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?chunk-pix]

(ns forma.source.rain
  (:use cascalog.api
        [forma.hadoop.io :only (get-bytes)]
        [forma.hadoop.predicate :only (window->array
                                       add-fields)]
        [forma.source.modis :only (hv->tilestring)]
        [forma.reproject :only (project-to-modis
                                dimensions-for-step)])
  (:require [cascalog.ops :as c]
            [clojure.contrib.io :as io])
  (:import  [java.io File InputStream]
            [java.util.zip GZIPInputStream]))

;; ## Dataset Information
;;
;; We chose to go with PRECL for our rain analysis, though any of the
;; global datasets listed in the [IRI/LDEO](http://goo.gl/RmTKe)
;; Climate Data Library's [atmospheric data page](http://goo.gl/V5tUI)
;; would be great.
;;
;; More information on the dataset can be found at the NOAA PRECL
;; [info site] (http://goo.gl/V3gqv). The data are fully documented in
;; "Global Land Precipitation: A 50-yr Monthly Analysis Based on Gauge
;; Observations", found [here].(http://goo.gl/PVOr6).
;;
;; ### Data Processing
;;
;; The NOAA PRECL data comes in files of binary arrays, with one file
;; for each year. Each file holds 24 datasets, alternating between
;; precip. rate in mm/day and total # of gauges, gridded in WGS84 at a
;; 0.5 degree step. No metadata exists to mark byte offsets. We
;; process these datasets using input streams, so we need to know in
;; advance how many bytes to read off per dataset.

(def float-bytes (/ ^Integer (Float/SIZE)
                    ^Integer (Byte/SIZE)))

(defn floats-for-step
  "Length of the row of floats (in # of bytes) representing the earth
  at the specified spatial step."
  [step]
  (apply * float-bytes (dimensions-for-step step)))

(defn input-stream
  "Attempts to coerce the given argument to an InputStream, with added
  support for gzipped files. If the input argument does point to a
  gzipped file, the default buffer will be sized to fit one NOAA
  PREC/L binary data file at 0.5 degree resolution, or 24 groups
  of `(* 360 720)` floats. To make sure that the returned
  GZIPInputStream will fully read into a supplied byte array, see
  `forma.source.rain/force-fill`."
  [arg]
  (let [^InputStream stream (io/input-stream arg)
        rainbuf-size (* 24 (floats-for-step 0.5))]
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
  ([step ^InputStream stream]
     (let [arr-size (floats-for-step step)
           buf (byte-array arr-size)]
       (if (pos? (force-fill stream buf))
         (lazy-seq
          (cons buf (lazy-months step stream)))
         (.close stream)))))

;; Once we have the byte arrays returned by `lazy-months`, we need to
;; process each one into meaningful data. Java reads primitive arrays
;; using [big endian](http://goo.gl/os4SJ) format, by default. The
;; PRECL dataset was stored using [little endian](http://goo.gl/KUpiy)
;; floats, 4 bytes each. We define a function below that flips the
;; endian order of a sequence of bytes -- we use this to coerce groups
;; of little-endian bytes into big-endian floats.

(defn flipped-endian-float
  "Flips the endian order of the supplied byte sequence, and converts
  the sequence into a float."
  [bitseq]
  (->> bitseq
       (map-indexed (fn [idx bit]
                      (bit-shift-left
                       (bit-and bit 0xff)
                       (* 8 idx))))
       (reduce +)
       (Float/intBitsToFloat)))

(defn big-floats
  "Converts the supplied little-endian byte array into a seq of
  big-endian-ordered floats."
  [little-bytes]
  (vec (map flipped-endian-float
            (partition float-bytes little-bytes))))

;; As our goal is to process each file into the tuple-set described
;; above, we need to tag each month with metadata, to distinguish it
;; from other datasets, and standardize the field information for
;; later queries against all data. We hardcode the name "precl" for
;; the data here, and store the month as an index, for later
;; conversion to time period.

(defn make-tuple
  "Generates a 2-tuple for NOAA PREC/L months. We increment the index,
  in this case, to make the number correspond to a month of the year,
  rather than an index in a seq."
  [index month]
  (vector (inc index) (big-floats month)))

(defn rain-tuples
  "Returns a lazy seq of 2-tuples representing NOAA PREC/L rain
  data. Note that we take every other element in the lazy-months seq,
  skipping data concerning # of gauges."
  [step stream]
  (map-indexed make-tuple
               (take-nth 2 (lazy-months step stream))))

;; This is our first cascalog query -- we use `defmapcatop`, as we
;; need to split each file out into twelve separate tuples, one for
;; each month.

(defmapcatop [unpack-rain [step]]
  ^{:doc "Unpacks a PREC/L binary file for a given year, and returns a
lazy sequence of 2-tuples, in the form of (month, data). Assumes that
binary files are packaged as hadoop BytesWritable objects."}
  [stream]
  (let [bytes (get-bytes stream)]
    (rain-tuples step (input-stream bytes))))

(defn to-datestring
  "Processes an NOAA PRECL filename and integer month index, and
  returns a datestring of the format `yyyy-mm-dd`. Filename is assumed
  to by formatted as `precl_mon_v1.0.lnx.YYYY.gri0.5m`, with a single
  group of four digits representing the year."
  [filename month-int]
  (let [year (first (re-find #"(\d{4})" filename))
        month (format "%02d" month-int)]
    (apply str (interpose "-" [year month "01"]))))

;; The following functions bring everything together, making heavy use
;; of the functionality supplied by `forma.reproject`. Rather than
;; sampling PRECL data at the positions of every possible MODIS tile,
;; we allow the user to supply a seq of TileIDs for processing. (No
;; MODIS product covers every possible tile; any product containing
;; the [NDVI](http://goo.gl/kjojh) only covers land, for example.)
;;
;; These functions are designed to be able to map between WGS84 and
;; MODIS datasets at arbitrary resolution. This explains the
;; resolution parameters seen in the functions below.
;;
;; (Note our use of `clojure.core/identity` in the following query;
;; since `identity` returns its argument, and every tuple is processed
;; through each operation, `identity` acts as a `defmapop` that adds a
;; new field onto a tuple. In this case, we know that our rain data
;; comes in monthly temporal resolution, and we need it to have some
;; dataset identifier, so we hardcode these fields onto every tuple.)

(defn rain-months
  "Cascalog subquery to extract all months from a directory of PREC/L
  datasets with the supplied map of WGS84 dataset information, paired
  with a datestring of the format `yyyy-mm-dd`. (See `forma.static`
  for examples of well-formed wgs84 dataset maps.)

  Source can be any tap that supplies PRECL files named
  precl_mon_v1.0.lnx.YYYY.gri0.5m(.gz, optionally)."
  [ascii-info source]
  (let [step (:step ascii-info)]
    (<- [?dataset ?temporal-res ?date ?raindata]
        (source ?filename ?file)
        (unpack-rain [step] ?file :> ?month ?raindata)
        (to-datestring ?filename ?month :> ?date)
        (add-fields "precl" "32" :> ?dataset ?temporal-res))))

;; Something interesting occurs in the next few functions. Rather than
;; combine `cross-join` and `fancy-index` into one query, we've
;; decided to break them out into subqueries, joined together by one
;; larger function that routes them through an intermediate
;; sequencefile. What's going on here?
;;
;; We have a method of producing sampling our the tiles we want from a
;; clojure data structure, and a method of unpacking rain-data for the
;; entire globe, for each month. To maintain the ability to
;; parallelize the sampling process, we need to pair up every tile
;; with every rain-month.

;; As described in [this thread](http://goo.gl/TviNn) on
;; [cascalog-user](http://goo.gl/Tqihs), this is called a cross-join;
;; we're taking the [cartesian product](http://goo.gl/r5Qsw) of the
;; two sets. We accomplish this by feeding every tuple into
;; `forma.hadoop.predicate/add-fields` with argument `m-res`, the
;; supplied MODIS resolution into which we're translating the PRECL
;; sets. `add-fields` produces the common dynamic variable
;; `?spatial-res`, needed by `fancy-index`.

(defn cross-join
  "Unpacks all NOAA PRECL files (at the supplied resolution) located
  at `source`, and cross-joins each month produced with each unique
  MODIS tile referenced within `tile-seq`."
  [m-res ascii-info tile-seq source]
  (let [tiles (memory-source-tap tile-seq)
        precl (rain-months ascii-info source)]
    (<- [?dataset ?spatial-res ?temporal-res ?mod-h ?mod-v ?date ?raindata]
        (tiles ?mod-h ?mod-v)
        (precl ?dataset ?temporal-res ?date ?raindata)
        (add-fields m-res :> ?spatial-res))))

;; The problem with cross-joining is that every tuple being joined is
;; joined by a single reducer. We need to map the index sequences onto
;; the rain data to produce our chunks, but we don't want the single
;; reducer to take on the entire job. As Nathan Marz suggests [in this
;; post](http://goo.gl/2EqHh), we can solve this problem by storing
;; all tuples into an intermediate sequencefile, forcing cascading to
;; create a new MapReduce task for the fancy-indexing. On one machine,
;; this slows performance slightly. On a cluster, it should give us a
;; big bump.

(defn fancy-index
  "Reduction step to process all tuples produced by
  `forma.source.rain/cross-join`. We break this out into a separate
  query because the cross-join forces all tuples to be processed by a
  single reducer. This step allows the flow to branch out again from
  that bottleneck, when run on a cluster."
  [m-res ascii-map chunk-size source]
  (<- [?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?chunk]
      (source ?dataset ?spatial-res ?temporal-res ?mod-h ?mod-v ?date ?raindata)
      (hv->tilestring ?mod-h ?mod-v :> ?tilestring)
      (project-to-modis [m-res ascii-map chunk-size]
                        ?raindata ?mod-h ?mod-v :> ?chunkid ?chunk)
      (window->array [Float/TYPE] ?chunk :> ?float-chunk)))

;; Finally, the chunker. This subquery is analogous to
;; `forma.source.hdf/modis-chunks`. This is the only subquery that
;; needs to be called, when processing new PRECL data.

(defn rain-chunks
  "Cascalog subquery to fully process a WGS84 float array at the
  supplied resolution (`:step`, within `ascii-map`) into tuples
  suitable for comparison to any MODIS dataset at the supplied modis
  resolution `m-res`, partitioned by the supplied chunk size."
  [m-res ascii-map chunk-size tile-seq source]
  (let [rnd (int (* 100000 (rand)))
        tmp (hfs-seqfile (str "/tmp/" rnd))]
    (?- tmp (cross-join m-res ascii-map tile-seq source))
    (fancy-index m-res ascii-map chunk-size tmp)))

;; An alternate formulation of `rain-chunks` that skipped the
;; intermediate sequencefile would be:
;;
;;     (defn rain-chunks [m-res ascii-map chunk-size tile-seq source]
;;       (let [precl (cross-join m-res ascii-map tile-seq source)]
;;         (fancy-index m-res ascii-map chunk-size precl)))
;;
;; This is worth testing on the cluster, though I suspect it simply
;; won't branch out to multiple reduce jobs, leaving one poor machine
;; to do all of the work. If successful, fancy-index should be
;; merged into this function, which should replace the intermediate
;; sequencefile solution.
