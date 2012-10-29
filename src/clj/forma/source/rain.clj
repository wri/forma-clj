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
        [clojure.contrib.math :only (floor)]
        [clojure.contrib.combinatorics :only (cartesian-product)]
        [forma.hadoop.io :only (hfs-wholefile)]
        [forma.hadoop.jobs.forma :only (consolidate-timeseries)])
  (:require [forma.reproject :as r]
            [forma.utils :as u]
            [cascalog.io :as io]
            [forma.source.static :as static]
            [forma.hadoop.predicate :as p]
            [forma.date-time :as date]
            [clojure.string :as s])
  (:import  [java.nio ByteBuffer ByteOrder]))

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
;; function that converts an array of little-endian bytes into an
;;  array of big-endian floats.

(defn tupleize [idx arr]
  (let [buf (-> (ByteBuffer/wrap arr)
                (.order ByteOrder/LITTLE_ENDIAN)
                (.asFloatBuffer))
        ret (float-array (.limit buf))]
    (.get buf ret)
    [(inc idx) ret]))

(defn rain-tuples
  "Returns a lazy seq of 2-tuples representing NOAA PREC/L rain
  data. Each 2-tuple is of the form `[idx, month-arr]`, where `idx`
  is the 1-based month and `month-vec` is a `(* 720 360)` float-array.

  Note that we take every other element in the `partition-stream` seq,
  skipping data concerning # of gauges."
  [step stream]
  (let [n (floats-for-step step)]
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

(defn all-nodata?
  "Check whether every element in a collection is a nodata value.

  Used to identify and ignore months in the rain data where there are
  no useful values. This happens when, for example, the raw file is
  updated in May and there is obviously no useful data for the rest of
  the year. Those months should be dropped until they become available
  later."
  [nodata coll]
  (every? (partial == nodata) coll))

(defn rain-values
  "Generates a cascalog subquery from the supplied WGS84 step size and
  source of `?filename` and `?file` tiles; this subquery returns
  individual rain values, marked by `?row`, `?col` and `?val` from the
  original WGS84 matrix. A date field is also included."
  [step nodata source]
  (let [unpack (rain-months step)]
    (<- [?date ?row ?col ?val]
        (source ?filename ?file)
        (unpack ?filename ?file :> ?date ?raindata)
        (all-nodata? nodata ?raindata :> false)
        (to-rows [step] ?raindata :> ?row ?row-data)
        (p/index ?row-data :> ?col ?val))))

(defn rain-rowcol->modispos
  "Accepts the row and column of a rain pixel (strangely defined
  according to the PREC/L data set) and returns the MODIS tile and
  position within the tile of the rain pixel.  Note that there are 20
  rain pixels on each side of a modis tile, so that the rain grid is
  of dimension 360x720."
  [r c]
  {:pre [(< r 360) (< c 720)]
   :post [(< (last %) 20)]}
  (let [v (- 17 (floor (/ r 20)))
        h (mod (+ 18 (floor (/ c 20))) 36)
        global-r (- 359 r)
        global-c (mod (+ 360 c) 720)
        tile-row (- global-r (* 20 v))
        tile-col (- global-c (* 20 h))]
    [h v tile-col tile-row]))

(defn rainpos->modis-range
  "Accepts a pixel-level spatial resolution (string: e.g., \"500\")
  and a row and column position of a rain pixel within a MODIS tile,
  and returns the horizontal and vertical extents of the MODIS pixels
  that fall within the rain pixel."
  [s-res tile-row tile-col]
  (let [num-pix ((keyword s-res) {:500 120 :1000 60 :250 240})
        init-col (* num-pix tile-col)
        init-row (* num-pix tile-row)]
    [[init-row (+ num-pix init-row)]
     [init-col (+ num-pix init-col)]]))

(defn fill-rect
  "accepts two tuples of length 2 that define the extent of a
  rectangle.  returns all integer pairs within the rectangle.  The
  ranges should be zero-indexed."
  [row-range col-range]
  {:pre [(= (count col-range) 2)]}
  (let [[rs cs] (map (partial apply range) [row-range col-range])]
    (cartesian-product rs cs)))

(defmapcatop explode-rain
  "Accepts the row and column of a rain pixel, as defined by the
  PRECL data set, along with the rain time series associated with
  that pixel.  Returns the series and coordinates (h, v, s, l) for all
  MODIS pixels within the rain pixel.  Note that this also depends on
  the spatial resolution, which should be supplied as a string."
  [rain-row rain-col series [s-res]]
  (let [[h v tile-row tile-col] (rain-rowcol->modispos rain-row rain-col)]
    (map (partial apply conj [h v series])
         (apply fill-rect (rainpos->modis-range s-res tile-row tile-col)))))

(defn mk-rain-series
  "Given a source of semi-raw rain data (date, row, column and value),
   plus a nodata value, returns a timeseries of values for each rain
   pixel."
  [src nodata]
  (let [t-res "32"]
    (<- [?row ?col ?series]
        (src ?date ?row ?col ?val)
        (date/datetime->period t-res ?date :> ?period)
        (double ?val :> ?val-dbl)
        (consolidate-timeseries nodata ?period ?val-dbl :> _ ?series))))

(defn rain-tap
  "Accepts a source of rain pixels and their associated rain time
  series, along with a spatial resolution, and returns a tap that
  assigns the series to all MODIS pixels within the rain pixel."
  [rain-src s-res]
  (let [series-src (mk-rain-series rain-src -9999.0)]
    (<- [?mod-h ?mod-v ?sample ?line ?series]
        (series-src ?row ?col ?series)
        (explode-rain ?row ?col ?series [s-res] :> ?mod-h ?mod-v ?series ?sample ?line)
        (:distinct false))))

(defn resample-rain
  "A Cascalog query that takes a tap emitting rain tuples and a tap emitting
  MODIS pixels for a set of MODIS tiles and emits the same rain tuples
  resampled into MODIS pixel coordinates.

  For example, given a rain tuple [date row col value]:

    [2000-01-01 239 489 100]

  The corresponding rain tuple [h v sample line] in MODIS pixel coordinates is:

    [8 6 0 0]

  Definitely note that there's a join happening in this query! The value of ?row
  and ?col must be the same wherever it is used, and since rain-vals and
  wgs84-indexer are separate sources of ?row and ?col, Cascalog will use an
  implicit join to resolve the query. Powerful stuff!

  Arguments:
    m-res - The MODIS resolution as a string.
    ascii-map - A map containing values for step and nodata values.
    file-tap - Cascalog generator that emits rain tuples  [date row col value]. 
    pix-tap - Cascalog generator that emits MODIS pixel tuples [h v sample line].
    args - Optional memory file-tap of rain tuples used for testing.

  Query result variables:
    ?dataset - The dataset name (precl)
    ?m-res - The MODIS spatial resolution in meters.
    ?t-res - The MODIS temporal resolution in days.
    !date - The nullable date of the rain tuple.
    ?mod-h - The MODIS h tile coordinate.
    ?mod-v - The MODIS v tile coordinate.
    ?sample - The MODIS tile sample coordinate.
    ?line - The MODIS tile line coordinate.
    ?val - The rain value.

  Example usage:
    > (??- 
        (let [tile-seq #{[8 6]}
             file-tap nil
             test-rain-data [[\"2000-01-01\" 239 489 100]] 
             pix-tap [[8 6 0 0]]
             ascii-map {:corner [0 -90] :travel [+ +] :step 0.5 :nodata -999}
             m-res \"500\"]
            (resample-rain m-res ascii-map file-tap pix-tap test-rain-data)))"
  [m-res {:keys [step nodata] :as ascii-map} file-tap pix-tap & args]
  (let [[test-rain-vals] args
        rain-vals (if (not test-rain-vals) (rain-values step nodata file-tap) test-rain-vals)
        mod-coords ["?mod-h" "?mod-v" "?sample" "?line"]]    
    (<- [?dataset ?m-res ?t-res !date ?mod-h ?mod-v ?sample ?line ?val]
        (rain-vals !date ?row ?col ?float-val)
        (double ?float-val :> ?val)
        (pix-tap :>> mod-coords)
        (p/add-fields "precl" "32" m-res :> ?dataset ?t-res ?m-res)
        (r/wgs84-indexer :<< (into [m-res ascii-map] mod-coords) :> ?row ?col))))

(defn rain-chunks
  "Cascalog subquery to fully process a WGS84 float array at the
  supplied resolution (`:step`, within `ascii-map`) into tuples
  suitable for comparison to any MODIS dataset at the supplied modis
  resolution `m-res`, partitioned by the supplied chunk size."
  [m-res {:keys [nodata] :as ascii-map} chunk-size file-tap pix-tap]
  (-> (resample-rain m-res ascii-map file-tap pix-tap)
      (static/agg-chunks m-res chunk-size nodata)))
