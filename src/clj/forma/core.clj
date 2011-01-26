(ns forma.core
  (:use cascalog.api)
  (:import [forma WholeFile])
  (:require [cascalog [vars :as v] [ops :as c] [workflow :as w]]
            [forma [hdf :as h]]))

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


;; ##Helper Functions

;; We currently can't have a subquery that returns any sort
;; of custom dataset. Once I get a response on the cascalog user group,
;; we need to go ahead and refactor this.
(defn unpacked-modis
  "Returns a stream of 1-tuples containing serialized gdal Datasets."
  [nasa-dir]
  (let [nasa-files (all-files nasa-dir)]
    (<- [?unpacked]
        (nasa-files ?hdf)
        (h/unpack ?hdf :> ?unpacked))))

;; ## Example Queries
;; This is the good stuff!

(defn add-metadata
  "Takes a seq of keys and an unpacked modis tile, and returns
   a number of fields based on the size of the input sequence."
  [modis keys]
  (let [outargs (v/gen-nullable-vars (count keys))]
    (<- outargs (modis ?freetile)
        (h/meta-values [keys] ? :>> outargs))))

(defn same-tiles
  "Refactored version of unique tiles."
  [nasa-dir]
  (let [nasa-files (unpacked-modis nasa-dir)
        meta-vals (add-metadata nasa-files ["TileID"])]
    (?<- (stdout) [?tileid ?count]
         (meta-vals ?tileid)
         (c/count ?count))))

(defn file-count
  "Prints the total count of files in a given directory to stdout."
  [dir]
  (let [files (all-files dir)]
    (?<- (stdout) [?count]
     (files ?file)
     (c/count ?count))))

(defn modis-chunks
  "Chunker."
  [hdf-source]
  (let [keys ["TileID"]]
    (<- [?tileid]
        (hdf-source ?hdf)
        (h/unpack ?hdf :> ?freetile)
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