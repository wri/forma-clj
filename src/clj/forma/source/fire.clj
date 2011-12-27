(ns forma.source.fire
  (:use cascalog.api
        [forma.date-time :only (convert)])
  (:require [clojure.string :as s]
            [forma.utils :as utils]
            [forma.schema :as schema]
            [forma.reproject :as r]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p])
  (:import [forma.schema FireTuple]))

;; ### Thrift Manipulation
;;
;; The fires dataset is different from previous MODIS datasets in that
;; it requires us to keep track of multiple values for each MODIS
;; pixel. We must maintain a total fire count, as well as a count of
;; the subsets of the total that satisfy certain conditions, such as
;; `Temp > 330 Kelvin`, `Confidence > 50`, or both at once. We
;; abstract this complication away by wrapping up each of these into a
;; clojure map. For example:
;;
;;    {:temp-330 1
;;     :conf-50 1
;;     :both-preds 1
;;     :count 2}
;;
;; compound value, represented as a `FireTuple` thrift object. We wrap
;; up collections of fire maps into a `FireSeries` thrift
;; object.

;; ### Fire Predicates

(defn daily-datestring
  "Takes a datestring from our daily fire datasets, formatted as
  `MM/DD/YYYY`, and returns a date formatted as `YYYY-MM-DD`."
  [datestring]
  (let [[month day year] (s/split datestring #"/")]
    (s/join "-" [year month day])))

(defn monthly-datestring
  "Takes a datestring from our monthly fire datasets, formatted as
  `YYYYMMDD`, and returns a date formatted as `YYYY-MM-DD`."
  [datestring]
  (convert datestring :basic-date :year-month-day))

(def fire-characteristics
  "Predicate macro that converts confidence and temperature into a
   tuple of fire characteristics."
  (<- [?conf ?kelvin :> ?tuple]
      (p/full-count ?conf :> ?count)
      (p/filtered-count [330] ?kelvin :> ?temp-330)
      (p/filtered-count [50] ?conf :> ?conf-50)
      (p/bi-filtered-count [50 330] ?conf ?kelvin :> ?both-preds)
      (schema/fire-value ?temp-330 ?conf-50 ?both-preds ?count :> ?tuple)))

;; ## Fire Queries

(def fire-pred
  (<- [?s-lat ?s-lon ?s-kelvin ?s-conf :> ?dataset ?t-res ?lat ?lon ?tuple]
      (utils/strings->floats ?s-lat ?s-lon ?s-kelvin ?s-conf
                             :> ?lat ?lon ?kelvin ?conf)
      (p/add-fields "fire" "01" :> ?dataset ?t-res)
      (fire-characteristics ?conf ?kelvin :> ?tuple)))

(defn fire-source-monthly
  "Takes a source of monthly fire textlines from before , and returns
  tuples with dataset, date, position and value all defined. In this
  case, the value `?tuple` is a `FireTuple` thrift object containing
  all relevant characteristics of fires for that particular day."
  [src]
  (<- [?dataset ?date ?t-res ?lat ?lon ?tuple]
      (src ?line)
      (p/mangle [#"\s+"] ?line :> ?datestring _ _ ?s-lat ?s-lon ?s-kelvin _ _ _ ?s-conf)
      (not= "YYYYMMDD" ?datestring)
      (monthly-datestring ?datestring :> ?date)
      (fire-pred ?s-lat ?s-lon ?s-kelvin ?s-conf :> ?dataset ?t-res ?lat ?lon ?tuple)
      (:distinct false)))

(defn fire-source-daily
  "Takes a source of textlines, and returns tuples with dataset, date,
  position and value all defined. In this case, the value `?tuple` is
  a `FireTuple` thrift object containing all relevant characteristics
  of fires for that particular day."
  [src]
  (<- [?dataset ?date ?t-res ?lat ?lon ?tuple]
      (src ?line)
      (p/mangle [#","] ?line :> ?s-lat ?s-lon ?s-kelvin _ _ ?datestring _ _ ?s-conf _ _ _)
      (daily-datestring ?datestring :> ?date)
      (fire-pred ?s-lat ?s-lon ?s-kelvin ?s-conf :> ?dataset ?t-res ?lat ?lon ?tuple)
      (:distinct false)))

(defn reproject-fires
  "Aggregates fire data at the supplied path by modis pixel at the
  supplied resolution."
  [m-res src]
  (<- [?datachunk]
      (p/add-fields m-res :> ?m-res)
      (src ?dataset ?date ?t-res ?lat ?lon ?tuple)
      (r/latlon->modis ?m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
      (io/pixel-location ?m-res ?mod-h ?mod-v ?sample ?line :> ?location)
      (io/mk-chunk ?dataset ?t-res ?date ?location ?tuple :> ?datachunk)
      (:distinct false)))
