(ns forma.core
  (:use cascalog.api
        (forma [sources :only (all-files files-with-names)]
               [conversion :only (to-period)]))
    (:require (cascalog [vars :as v]
                        [ops :as c])
            (forma [hdf :as h]
                   [rain :as r])))


;; ## Example Queries

(defmapop
  #^{:doc "implements a re-find, and returns the matches, not
the original string."}
  [re-group [pattern]] [str]
  (let [matches (re-find pattern str)]
    (rest matches)))

(defn modis-chunks
  "Currently returns dataset, tile, period - no data! Soon, this will
  actually stream chunks of MODIS data out into the world."
  [hdf-source]
  (let [keys ["TileID" "PRODUCTIONDATETIME"]]
    (<- [?dataset ?tile ?period]
        (hdf-source ?hdf)
        (h/unpack ?hdf :> ?dataset ?freetile)
        (h/meta-values [keys] ?freetile :> ?tileid ?juliantime)
        (re-group [#"(\d{2})(\d{6})"] ?tileid :> ?prefix ?tile)
        (to-period ?juliantime :> ?period)
        (:distinct false))))

(defn tile-metadata
  "Processes all HDF files in the supplied directory, and prints the
dataset names, tiles, time periods, and their associated counts to
standard out."
  [nasa-dir]
  (let [nasa-files (all-files nasa-dir)
        metadata (modis-chunks nasa-files)]
    (?<- (stdout) [?dataset ?tile ?period ?count]
         (metadata ?dataset ?tile ?period)
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