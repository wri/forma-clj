(ns forma.core
    (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]
           [forma WholeFile])
  (:require [cascalog [ops :as c] [workflow :as w]]
            [forma [hdf :as h]]))

(defn whole-file
  "Custom scheme for dealing with entire files."
  [field-name]
  (WholeFile. (w/fields field-name)))

(defn hfs-wholefile
  "Creates a tap on HDFS using the wholefile format. Guaranteed not
to chop files up, nice for unsupported compression formats like HDF."
  [path]
  (w/hfs-tap (whole-file ["file"]) path))

(defn nasa-data
  "Subquery to return all HDF files in the supplied directory."
  [dir]
  (let [source (hfs-wholefile dir)]
    (<- [?file] (source ?file))))

(defn hdf-count
  "totals up all HDF files in a given directory."
  [nasa-dir]
  (let [nasa (nasa-data nasa-dir)]
    (?<- (stdout) [?count]
     (nasa ?file)
     (c/count ?count))))

(defn get-tileid [file]
  (let [hdf (h/unpack-hdf file)]
    (h/get-metadata hdf "TileID")))

(defn unique-tiles
  "figures out unique tiles for all HDF files in a given directory."
  [nasa-dir]
  (let [nasa (nasa-data nasa-dir)]
    (?<- (stdout) [?tileid ?count]
         (nasa ?tile)
         (get-tileid ?tile :> ?tileid) 
         (c/count ?count))))
