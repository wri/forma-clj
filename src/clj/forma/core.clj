(ns forma.core
  (:use cascalog.api)
  (:import [forma WholeFile])
  (:require [cascalog [ops :as c] [workflow :as w]]
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

(defn file-count
  "Prints the total count of files in a given directory to stdout."
  [dir]
  (let [files (all-files dir)]
    (?<- (stdout) [?count]
     (files ?file)
     (c/count ?count))))

(defn unique-tiles
  "Processes all HDF files in the supplied directory, and prints the TileIDs
   and their associated counts to standard out."
  [nasa-dir]
  (let [nasa-files (all-files nasa-dir)]
    (?<- (stdout) [?tileid ?count]
         (nasa-files ?hdf)
         (h/unpack ?hdf :> ?freetile)
         (h/get-tileid ?freetile :> ?tileid) 
         (c/count ?count))))