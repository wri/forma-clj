(ns forma.conversion
  (:use (cascalog [api :only (defmapop)])
        (clojure.contrib.generic [math-functions :only (ceil)])
        (clj-time [core :only (date-time in-minutes interval)]
                  [coerce :only (from-string)]))
  (:import [org.joda.time DateTime]))

(set! *warn-on-reflection* true)

;; TODO -- update this for 250 meter data, and take resolution as an
;; input. (I think the period is 16 days.) This is as easy as changing
;; period-length to a map -- then we can call (period-length res) and
;; get the proper value.

(def modis-start (date-time 2000))

(def
  ^{:doc "Length of a period, according to FORMA, keyed by MODIS
  resolution."}
  period-length
  {"1000" 32.0
   "250" 16.0})

(defn in-days
  "Returns the number of days spanned by the given interval."
  [interval]
  (let [mins (in-minutes interval)
        hours (/ mins 60)
        days (/ hours 24)]
    (int days)))

(defn in-periods [length interval]
  (let [days (in-days interval)
        period (/ days length)]
    ((comp int ceil) period)))

(defmulti julian->period
  "Converts a given input date into an interval from the MODIS
  beginnings, as specified by modis-start. Periods begin counting from
  1. This function accepts DateTime objects, strings that can be
  coerced into dates, or an arbitrary number of integer pieces. See
  clj-time's date-time and from-string for more information."
  (fn [x & _] (type x)))

(defmethod julian->period DateTime
  [date res]
  (inc (in-periods (period-length res)
                   (interval modis-start date))))

(defmethod julian->period String
  [str res]
  (julian->period (from-string str) res))

(defmethod julian->period Integer
  [& args]
  (let [res (last args)
        int-pieces (butlast args)]
    (julian->period (apply date-time int-pieces) res)))

;; Cascalog query to access multimethod, until issue gets fixed.
(defmapop to-period [date res]
  (julian->period date res))