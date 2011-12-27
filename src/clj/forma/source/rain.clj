;; this namespace provides functions for unpacking binary files of
;; NOAA PRECL data at 0.5 degree resolution, and processing them into
;; tuples suitable for machine learning against MODIS data.
;;
;; As in `forma.source.hdf`, the overall goal here is to process an
;; NOAA PREC/L dataset into tuples of the form
;;
;;     [?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?chunk-pix]

(ns forma.source.rain
  (:use cascalog.api)
  (:require [forma.reproject :as r]
            [forma.utils :as u]
            [cascalog.io :as io]
            [forma.source.static :as static]
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

(defn floats-for-step
  "Length of the row of floats (in # of bytes) representing the earth
  at the specified spatial step."
  [step]
  (apply * u/float-bytes (r/dimensions-for-step step)))

;; ### Data Extraction

;; Java reads primitive arrays using [big endian](http://goo.gl/os4SJ)
;; format, by default. The PRECL dataset was stored using [little
;; endian](http://goo.gl/KUpiy) floats, 4 bytes each. We define a
;; function that converts an array of little-endian bytes into a
;; vector of big-endian floats.

(defn big-floats
  "Converts the supplied little-endian byte array into a seq of
  big-endian-ordered floats."
  [little-bytes]
  (vec (map u/flipped-endian-float
            (partition u/float-bytes little-bytes))))

(defn rain-tuples
  "Returns a lazy seq of 2-tuples representing NOAA PREC/L rain
  data. Each 2-tuple is of the form `[idx, month-vec]`, where `idx` is
  the 1-based month and `month-vec` is a vector of `(* 720 360)`
  big-endian floats.

  Note that we take every other element in the `partition-stream` seq,
  skipping data concerning # of gauges."
  [step stream]
  (let [n (floats-for-step step)
        tupleize (fn [idx arr]
                   [(inc idx) (big-floats arr)])]
    (->> stream
         (u/partition-stream n)
         (take-nth 2)
         (map-indexed tupleize))))

(defmapcatop [unpack-rain [step]]
  "Unpacks a PREC/L binary file for a given year, and returns a lazy
  sequence of 2-tuples, in the form of (month, data). Assumes that
  binary files are packaged as hadoop BytesWritable objects."
  [stream]
  (let [bytes        (io/get-bytes stream)
        rainbuf-size (* 24 (floats-for-step 0.5))]
    (->> (u/input-stream bytes rainbuf-size)
         (rain-tuples step))))

(defn to-datestring
  "Processes an NOAA PRECL filename and integer month index, and
  returns a datestring of the format `yyyy-mm-dd`. Filename is assumed
  to by formatted as `precl_mon_v1.0.lnx.YYYY.gri0.5m`, with a single
  group of four digits representing the year."
  [filename month-int]
  {:post [(= 10 (count %))]}
  (let [year (first (re-find #"(\d{4})" filename))
        end (format "-%02d-01" month-int)]
    (str year end)))

(defn rain-months
  "Generates a predicate macro to extract all months from a directory
  of PREC/L datasets, paired with a datestring of the format
  `yyyy-mm-dd`.  Filename must be of the format

    precl_mon_v1.0.lnx.YYYY.gri0.5m(.gz, optionally)."
  [step]
  (<- [?filename ?file :> ?date ?raindata]
      (unpack-rain [step] ?file :> ?month ?raindata)
      (to-datestring ?filename ?month :> ?date)))

;; TODO: Merge into hadoop.predicate. We want to generalize that
;; pattern of taking a 2d array and cutting it up into pixels.
(defmapcatop [to-rows [step]]
  "Converts a month's worth of PRECL data, stored in a vector,
  into single rows of data, based on the supplied step size. `to-rows`
  outputs 2-tuples of the form `[row-idx, row-array]`."
  [coll]
  (let [[row-length] (r/dimensions-for-step step)]
    (->> coll
         (partition row-length)
         (map-indexed vector))))

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
        (p/index 0 ?row-data :> ?col ?val))))

(defn resample-rain
  "Cascalog query that merges the `?row` `?col` and `?val` generated
  by `rain-values` with the MODIS coordinate tuples generated by
  `pix-tap`. Cascalog does the heavy lifting here, simply because of
  the common name between the generated and supplied `?row` and `?col`
  fields. Powerful stuff!"
  [m-res {:keys [step nodata] :as ascii-map} file-tap pix-tap]
  (let [rain-vals (rain-values step file-tap)
        mod-coords ["?mod-h" "?mod-v" "?sample" "?line"]]
    (<- [?dataset ?m-res ?t-res !date ?mod-h ?mod-v ?sample ?line ?val]
        (rain-vals !date ?row ?col ?val)
        (not= ?val nodata)
        (pix-tap :>> mod-coords)
        (p/add-fields "precl" "32" m-res :> ?dataset ?t-res ?m-res)
        (r/wgs84-indexer :<< (into [m-res ascii-map] mod-coords) :> ?row ?col)
        (:distinct false))))

(defn rain-chunks
  "Cascalog subquery to fully process a WGS84 float array at the
  supplied resolution (`:step`, within `ascii-map`) into tuples
  suitable for comparison to any MODIS dataset at the supplied modis
  resolution `m-res`, partitioned by the supplied chunk size."
  [m-res {:keys [nodata] :as ascii-map} chunk-size file-tap pix-tap]
  (-> (resample-rain m-res ascii-map file-tap pix-tap)
      (static/agg-chunks m-res chunk-size nodata :int)))
