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
        [forma.source.modis :only (hv->tilestring chunk-dims)]
        [forma.reproject :only (wgs84-indexer
                                dimensions-for-step)])
  (:require [forma.utils :as u]
            [forma.hadoop.predicate :as p]
            [clojure.string :as s])
  (:import  [java.io InputStream]))

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

;; ### Data Extraction

;; TODO: Turn into `stream-partition` and move to utils.
(defn lazy-months
  "Generates a lazy seq of byte-arrays from a binary NOAA PREC/L
  file. Elements alternate between precipitation rate in mm / day and
  total # of gauges."
  ([step ^InputStream stream]
     (let [arr-size (floats-for-step step)
           buf (byte-array arr-size)]
       (if (pos? (u/force-fill stream buf))
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

;; TODO: Move to utils under its own section.
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
  [(inc index) (big-floats month)])

(defn rain-tuples
  "Returns a lazy seq of 2-tuples representing NOAA PREC/L rain
  data. Note that we take every other element in the lazy-months seq,
  skipping data concerning # of gauges."
  [step stream]
  (->> (lazy-months step stream)
       (take-nth 2)
       (map-indexed make-tuple)))

;; This is our first cascalog query -- we use `defmapcatop`, as we
;; need to split each file out into twelve separate tuples, one for
;; each month.

(defmapcatop [unpack-rain [step]]
  ^{:doc "Unpacks a PREC/L binary file for a given year, and returns a
lazy sequence of 2-tuples, in the form of (month, data). Assumes that
binary files are packaged as hadoop BytesWritable objects."}
  [stream]
  (let [bytes (get-bytes stream)
        rainbuf-size (* 24 (floats-for-step 0.5))]
    (->> (u/input-stream bytes rainbuf-size)
         (rain-tuples step))))

(defn to-datestring
  "Processes an NOAA PRECL filename and integer month index, and
  returns a datestring of the format `yyyy-mm-dd`. Filename is assumed
  to by formatted as `precl_mon_v1.0.lnx.YYYY.gri0.5m`, with a single
  group of four digits representing the year."
  [filename month-int]
  (let [year (first (re-find #"(\d{4})" filename))
        month (format "%02d" month-int)]
    (s/join "-" [year month "01"])))

;; Rather than sampling PRECL data at the positions of every possible
;; MODIS tile, we allow the user to supply a seq of TileIDs for
;; processing. (No MODIS product covers every possible tile; any
;; product containing the [NDVI](http://goo.gl/kjojh) only covers
;; land, for example.)

(defn rain-months
  "Generates a predicate macro to extract all months from a directory
  of PREC/L datasets, paired with a datestring of the format
  `yyyy-mm-dd`.  Filename must be of the format

    precl_mon_v1.0.lnx.YYYY.gri0.5m(.gz, optionally)."
  [step]
  (<- [?filename ?file :> ?date ?raindata]
      (unpack-rain [step] ?file :> ?month ?raindata)
      (to-datestring ?filename ?month :> ?date)))

;; TODO: Merge into hadoop.predicate.
(defmapcatop
  ^{:doc "Converts a month's worth of PRECL data, stored in an
  int-array, into single rows of data, based on the supplied step
  size. `to-rows` outputs 2-tuples of the form `[row-idx,
  row-array]`."}
  [to-rows [step]]
  [coll]
  (let [[row-length] (dimensions-for-step step)]
    (->> coll
         (partition row-length)
         (map-indexed (fn [idx xs]
                        [idx (int-array xs)])))))

(defn rain-values
  "Generates a cascalog subquery from the supplied WGS84 step size and
  source of `?filename` and `?file` tiles; this subquery returns
  individual rain values, marked by `?row`, `?col` and `?val` from the
  original WGS84 matrix. A date field is also included."
  [step source]
  (let [unpack (rain-months step)]
    (<- [?date ?row ?col ?val]
        (source ?filename ?file)
        (unpack ?filename ?file :> ?date ?raindata)
        (to-rows [step] ?raindata :> ?row ?row-data)
        (p/index ?row-data :> ?col ?val))))

(defn resample-rain
  "Cascalog query that merges the `?row` `?col` and `?val` generated
  by `rain-values` with the MODIS coordinate tuples generated by
  `pix-tap`. Cascalog does the heavy lifting here, simply because of
  the common name between the generated and supplied `?row` and `?col`
  fields. Powerful stuff!"
  [m-res {:keys [step] :as ascii-map} file-tap pix-tap]
  (let [rain-vals (rain-values step file-tap)
        mod-coords ["?mod-h" "?mod-v" "?sample" "?line"]]
    (<- [?date ?mod-h ?mod-v ?sample ?line ?val]
        (rain-vals ?date ?row ?col ?val)
        (pix-tap :>> mod-coords)
        (wgs84-indexer :<< (into [m-res ascii-map] mod-coords) :> ?row ?col))))

(defn rain-chunks
  "Cascalog subquery to fully process a WGS84 float array at the
  supplied resolution (`:step`, within `ascii-map`) into tuples
  suitable for comparison to any MODIS dataset at the supplied modis
  resolution `m-res`, partitioned by the supplied chunk size."
  [m-res {:keys [nodata] :as ascii-map} chunk-size file-tap pix-tap]
  (let [[width height] (chunk-dims m-res chunk-size)
        window-src (-> (resample-rain m-res ascii-map file-tap pix-tap)
                       (p/sparse-windower ["?sample" "?line"]
                                          [width height]
                                          "?val"
                                          nodata))]
    (<- [?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?chunk]
        (window-src ?date ?mod-h ?mod-v _ ?chunkid ?window)
        (p/window->struct [:int] ?window :> ?chunk)
        (hv->tilestring ?mod-h ?mod-v :> ?tilestring)
        (p/add-fields "precl" "32" m-res :> ?dataset ?temporal-res ?spatial-res))))
