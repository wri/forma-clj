(ns forma.core
  (:use cascalog.api)
  (:import [forma WholeFile])
  (:require [cascalog [vars :as v] [ops :as c] [workflow :as w]]
            [forma [hdf :as h] [rain :as r]]))

;; ## Tap files
;; These help define the source tap that will slurp up HDF
;; files from a local file, remote file, or S3 bucket.

(defn whole-file
  "Custom scheme for dealing with entire files."
  [field-name]
  (WholeFile. (w/fields field-name)))

(defn hfs-wholefile
  "Creates a tap on HDFS using the wholefile format. Guaranteed not
   to chop files up! Required for unsupported compression formats like HDF."
  [path]
  (w/hfs-tap (whole-file ["file"]) path))

(defn all-files
  "Subquery to return all files in the supplied directory."
  [dir]
  (let [source (hfs-wholefile dir)]
    (<- [?file] (source ?file))))


;; ##Subqueries

(defn add-metadata
  "Takes a seq of keys and an unpacked modis tile, and returns
   a number of fields based on the size of the input sequence."
  [modis keys]
  (let [outargs (v/gen-nullable-vars (count keys))]
    (<- outargs (modis ?freetile)
        (h/meta-values [keys] ? :>> outargs))))

;; We currently can't have a subquery that returns any sort
;; of custom dataset. Once I get a response on the cascalog user group,
;; I'll go ahead and refactor this.

(defn unpacked-modis
  "Returns a stream of 1-tuples containing serialized gdal Datasets with their associated tags."
  [nasa-dir]
  (let [nasa-files (all-files nasa-dir)]
    (<- [?dataset ?unpacked]
        (nasa-files ?hdf)
        (h/unpack ?hdf :> ?dataset ?unpacked))))

;; ## Full Example Queries

(defn file-count
  "Prints the total count of files in a given directory to stdout."
  [dir]
  (let [files (all-files dir)]
    (?<- (stdout) [?count]
     (files ?file)
     (c/count ?count))))

(defn modis-chunks
  "Chunker currently returns the TileID alone.
   [TODO] figure out what metadata we need associated with each chunk."
  [hdf-source]
  (let [keys ["TileID"]]
    (<- [?tileid]
        (hdf-source ?hdf)
        (h/unpack ?hdf :> ?dataset ?freetile)
        (h/meta-values [keys] ?freetile :> ?tileid)
        (:distinct false))))

(defn unique-tiles
  "Processes all HDF files in the supplied directory, and prints the TileIDs
   and their associated counts to standard out."
  [nasa-dir]
  (let [nasa-files (all-files nasa-dir)
        chunks (modis-chunks nasa-files)]
    (?<- (stdout) [?tileid ?count]
         (chunks ?tileid)
         (c/count ?count))))

(defn same-tiles
  "Refactored version of unique tiles. NOT finished!
   [TODO] verify if add-metadata will work here."
  [nasa-dir]
  (let [nasa-files (unpacked-modis nasa-dir)]
    (?<- (stdout) [?tileid ?count]
         (nasa-files ?dataset ?unpacked)
         (add-metadata ?unpacked ["TileID"] :> ?tileid)
         (c/count ?count))))

(defn rain-months
  "Test query! Returns the count of output data sets for each month, from
   0 to 11."
  [rain-dir]
  (let [rain-files (all-files rain-dir)]
    (?<- (stdout) [?month ?count]
         (rain-files ?file)
         (r/rain-months ?file :> ?month ?month-data)
         (c/count ?count))))