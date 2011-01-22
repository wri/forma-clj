(ns forma.core
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]
           [forma WholeFile])
  (:require [cascalog [workflow :as w]]))

(def nasa-dir-path
  "/Users/sritchie/Desktop/MOD13A3.A2000032.h00v08.005.2006271174446.hdf")

(def test-dataset
  (do
    (gdal/AllRegister)
    (gdal/Open nasa-dir-path 0)))

(defn whole-file [field-name]
  (WholeFile. (w/fields field-name)))

(defn hfs-wholefile
  "Creates a tap on HDFS that takes full files as input."
  [path]
  (w/hfs-tap (whole-file ["file"]) path))

