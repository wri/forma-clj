(ns forma.conversion
  (:use (cascalog [api :only (defmapop)])
        (clojure.contrib.generic [math-functions :only (ceil)])
        (clj-time [core :only (date-time in-minutes interval)]
                  [coerce :only (from-string)]))
  (:import [org.joda.time DateTime]))

;; TODO -- update this for 250 meter data, and take resolution as an
;; input. (I think the period is 16 days.) This is as easy as changing
;; period-length to a map -- then we can call (period-length res) and
;; get the proper value.
(def modis-start (date-time 2000))
(def period-length 32.0)

(defn in-days
  "Returns the number of days spanned by the given interval."
  [interval]
  (let [mins (in-minutes interval)
        hours (/ mins 60)
        days (/ hours 24)]
    (int days)))

(defn in-periods [interval]
  (let [days (in-days interval)
        period (/ days period-length)]
    ((comp int ceil) period)))

(defmulti julian->period
    "Converts a given input date into an interval from the MODIS
  beginnings, as specified by modis-start. Accepts DateTime objects,
  strings that can be coerced into dates, or an arbitrary number of
  integer pieces. See clj-time's date-time and from-string for more
  information."
    (fn [x & _] (type x)))

(defmethod julian->period DateTime
  [date]
  (in-periods (interval modis-start date)))

(defmethod julian->period String
  [str]
  (julian->period (from-string str)))

(defmethod julian->period Integer
  [& int-pieces]
  (julian->period (apply date-time int-pieces)))

;; Cascalog query to access multimethod, until issue gets fixed.
(defmapop to-period [date]
  (julian->period date))