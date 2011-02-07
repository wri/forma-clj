(ns forma.core
  (:use cascalog.api
        (forma [sources :only (all-files)]
               conversion))
    (:require (cascalog [vars :as v]
                        [ops :as c])
            (forma [hdf :as h]
                   [rain :as r])))

;; ## FORMA Constants
;; Aside from our EC2 information, this is the only data we need to
;; supply for the first phase of operations. Everything else should be
;; able to remain local to individual modules.

(def
  ^{:doc "MODIS datasets required for FORMA processing."}  
  forma-subsets
  #{:ndvi :evi :qual :reli})

(def forma-res "1000")

(def
  ^{:doc "Arbitrary number of pixels slurped at a time off of a
MODIS raster band. For 1km data, each MODIS tile is 1200x1200 pixels;
dealing with each pixel individually would incur unacceptable IO costs
within hadoop. We currently fix the chunk size at 24,000, resulting in
60 chunks per 1km data. Sharper resolution -> more chunks!"}
  chunk-size 24000)

;; ## Demonstration Queries
;; This first one deals with MODIS data.

(defn chunk-test
  "Simple query that takes a directory containing MODIS HDF files, or
  a link directly to such a file, totals up the # of chunks per file,
  and displays the count alongside some other nice metadata. This
  method is a proof of concept for the hadoop chunking system; The
  fact that it works shows that chunking is occurring, and the
  individual chunks are being serialized over multiple hadoop jobs."
  [dir]
  (let [source (all-files dir)
        chunks (h/modis-chunks source forma-subsets chunk-size)]
    (?<- (stdout)
         [?dataset ?res ?tile-x ?tile-y ?period ?count]
         (chunks ?dataset ?res ?tile-x ?tile-y ?period ?chunkid ?chunk)
         (c/count ?count))))

;; Now, on to the rain data. We might want to move this over into
;; rain.

(defmapop [extract-period [res]]
  ^{:doc "Extracts the year from a NOAA PREC/L filename, assuming that
  the year is the only group of 4 digits. pairs it with the supplied
  month, and converts both into a time period, with units in months
  from the MODIS start date as defined in conversion.clj."}
  [filename month]
  (let [year (Integer/parseInt (first (re-find #"(\d{4})" filename)))]
    [res (julian->period year month res)]))

(defn rain-months
  "Test query! Returns the first few pieces of metadata for
rain. Currently, we'll get a dataset name, a MODIS resolution, and the
time period of the month datasets extracted from the input files. The
default resolution is forma-res, as defined in core."
  ([rain-dir]
     (rain-months rain-dir forma-res))
  ([rain-dir res]
     (let [rain-files (all-files rain-dir)]
       (?<- (stdout) [?dataset ?res ?period]
            (rain-files ?filename ?file)
            (extract-period [res] ?filename ?month :> ?res ?period)
            (r/unpack-rain ?file :> ?dataset ?month ?month-data)))))