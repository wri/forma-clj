(ns forma.core
  (:use cascalog.api
        (forma [sources :only (all-files files-with-names)]
               [conversion :only (to-period)]))
    (:require (cascalog [vars :as v]
                        [ops :as c])
            (forma [hdf :as h]
                   [rain :as r])))

(def
  #^{:doc "MODIS datasets required for FORMA processing."}
  forma-subsets
  #{:ndvi :evi :qual :reli})

;; ## Example Queries

;; TODO -- comment here on why we don't split this up into a
;; subquery. it's because the FORMA datasets themselves can't actually
;; be serialized over Hadoop, as no serializer can be registered. As
;; such, first we need to convert them into some sort of serializable
;; int array, plus associated metadata. This query (must be renamed)
;; will return the data array, plus all metadata that we'll want over
;; the course of the calculations.

;; The next step here will be to write a query that grabs data from a
;; number of different datasources, and aggregates them together.

;; TODO --some research on cascalog and cascading to see how we do a
;; join of data; we want a query that will give us absolutely all of
;; the chunks, for any dataset we like.

(defn modis-chunks
  "Current returns some metadata -- soon will return all chunks."
  [hdf-source]
  (let [keys ["TileID" "PRODUCTIONDATETIME"]]
    (<- [?dataset ?res ?tile-x ?tile-y ?period]
        (hdf-source ?hdf)
        (h/unpack [forma-subsets] ?hdf :> ?dataset ?freetile)
        (h/meta-values [keys] ?freetile :> ?tileid ?juliantime)
        (h/split-id ?tileid :> ?res ?tile-x ?tile-y)        
        (to-period ?juliantime :> ?period)
        (:distinct false))))

;; this guy shoul work.
(defn tile-metadata
  "Processes all HDF files in the supplied directory and prints
  metadata to stdout. Just here for testing purposes, for now."
  [nasa-dir]
  (let [nasa-files (all-files nasa-dir)
        metadata (modis-chunks nasa-files)]
    (?<- (stdout) [?dataset ?res ?tile-x ?tile-y ?period ?count]
         (metadata ?dataset ?res ?tile-x ?tile-y ?period)
         (c/count ?count))))

;; TODO -- convert filename and month into julian time period.
;; TODO -- Check the FORMA code to see how we decide time periods for
;; these bad boys. Do we just assume the first of the month?
(defn rain-months
  "Test query! Returns the count of output data sets for each month,
   from 0 to 11."
  [rain-dir]
  (let [rain-files (files-with-names rain-dir)]
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

;; We currently can't have a subquery that returns any sort
;; of custom dataset. Once I get a response on the cascalog user group,
;; I'll go ahead and refactor this.

(defn unpacked-modis
  "Returns a stream of 1-tuples containing serialized gdal Datasets
  with their associated tags."
  [nasa-dir]
  (let [nasa-files (all-files nasa-dir)]
    (<- [?dataset ?unpacked]
        (nasa-files ?hdf)
        (h/unpack ?hdf :> ?dataset ?unpacked))))

(defn add-metadata
  "Takes a seq of keys and an unpacked modis tile, and returns
   a number of fields based on the size of the input sequence."
  [modis keys]
  (let [outargs (v/gen-nullable-vars (count keys))]
    (<- outargs (modis ?freetile)
        (h/meta-values [keys] ? :>> outargs))))

(defn same-tiles
  "Refactored version of tile metadata. NOT finished! To get this
  done, the datasets themselves need a serializer. [TODO] verify if
  add-metadata will work here."
  [nasa-dir]
  (let [nasa-files (unpacked-modis nasa-dir)]
    (?<- (stdout) [?tileid ?count]
         (nasa-files ?dataset ?unpacked)
         (add-metadata ?unpacked ["TileID"] :> ?tileid)
         (c/count ?count))))