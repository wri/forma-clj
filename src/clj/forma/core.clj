(ns forma.core
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]
           [forma WholeFile])
  (:require [cascalog [ops :as c] [workflow :as w]]))

(def nasa-dir-path
  "/Users/sritchie/Desktop/MODISTEST/")

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

(def testquery
  (?<- (stdout) [?count]
     ((hfs-wholefile nasa-dir-path) ?file)
     (c/count ?count)))

