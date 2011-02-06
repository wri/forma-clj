(ns forma.core
  (:use cascalog.api
        (forma [sources :only (all-files)]))
    (:require (cascalog [vars :as v]
                        [ops :as c])
            (forma [hdf :as h]
                   [rain :as r])))

(def forma-subsets
  #^{:doc "MODIS datasets required for FORMA processing."}
  #{:ndvi :evi :qual :reli})

;; ## Test Queries

;; TODO -- comment here on why we don't split this up into a
;; subquery. it's because the FORMA datasets themselves can't actually
;; be serialized over Hadoop, as no serializer can be registered. As
;; such, first we need to convert them into some sort of serializable
;; int array, plus associated metadata. This query (must be renamed)
;; will return the data array, plus all metadata that we'll want over
;; the course of the calculations.

;; TODO --some research on cascalog and cascading to see how we do a
;; join of data; we want a query that will give us absolutely all of
;; the chunks, for any dataset we like.

;;  If this query works, almost all steps along the way also work --
;;  though, as of now, we do have the possibility that we'll have some
;;  error on the step where we read the raster into the array.

(defn chunk-test
  "Simple query that takes a directory containing MODIS HDF files, or
  a link directly to such a file, totals up the # of chunks per file,
  and displays the count alongside some other nice metadata."
  [dir]
  (let [source (all-files dir)
        chunks (h/modis-chunks source forma-subsets)]
    (?<- (stdout) [?dataset ?res ?tile-x ?tile-y ?period ?count]
         (chunks ?dataset ?res ?tile-x ?tile-y ?period ?chunkid ?chunk)
         (c/count ?count))))

;; TODO -- convert filename and month into julian time period.
;; TODO -- Check the FORMA code to see how we decide time periods for
;; these bad boys. Do we just assume the first of the month?
(defn rain-months
  "Test query! Returns the count of output data sets for each month,
   from 0 to 11."
  [rain-dir]
  (let [rain-files (all-files rain-dir)]
    (?<- (stdout) [?filename ?month ?count]
         (rain-files ?filename ?file)
         (r/rain-months ?file :> ?month ?month-data)
         (c/count ?count))))

;; ## Works in Progress

;; These are meant to be ignored for now -- we've make great use of
;; them once we have the ability to serialize a MODIS dataset using
;; hadoop. I just have to write the class that lets us do this, and
;; decide on a serialization mechanism. (I'm thinking that we'll go
;; well to simply serialize the float array, or int array, underlying
;; the whole thing.)