(ns forma.core
  (:use cascalog.api)
  (:import [org.gdal.gdal gdal Dataset]) ;; looks like use imports all symbols. Require just makes the ns available?
  (:require [cascalog [vars :as v] [ops :as c]]))

(def nasa-dir-path "/Users/sritchie/Desktop/MOD13A3.A2000032.h00v08.005.2006271174446.hdf")

(def test-dataset
  (do
    (gdal/AllRegister)
    (gdal/Open nasa-dir-path 0)))

;; (defn follows-data [dir]
;;   (let [source (hfs-textline dir)]
;;     (<- [?p ?p2] (source ?line) (re-parse [#"[^\s]+"] ?line :> ?p ?p2)
;;                      (:distinct false))))

;; Need to make some sort of HDF file format parser!!

;; (defn hfs-nasa
  ;; "Creates a tap on HDFS using custom format that doesn't split.

  ;;  See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
  ;;  http://www.cascading.org/javadoc/cascading/scheme/TextLine.html"
  ;; [path]
  ;; (w/hfs-tap (w/text-line ["line"] Fields/ALL) path))