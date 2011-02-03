;; First step will be to write a conversion between julian time and
;; the time periods we deal with in FORMA.

(ns forma.conversion
  (:use (clojure.contrib.generic [math-functions :only (ceil)])
        (clj-time [core :only (date-time in-minutes interval)]
                  [coerce :only (from-string)])))

;; TODO -- update this for 250 meter data. I think the period is half.
(def modis-start (date-time 2000))
(def period-length 32.0)

(defn in-periods [interval]
  (let [days (in-days interval)
        period (/ days period-length)]
    ((comp int ceil) period)))

(defn in-days
  "Returns the number of days spanned by the given interval."
  [interval]
  (let [mins (in-minutes interval)
        hours (/ mins 60)
        days (/ hours 24)]
    (int days)))

(defn julian->period
  "Converts a given input date string into an interval from the MODIS
  beginnings, as specified by modis-start."
  [str]
  (let [date (from-string str)]
    (in-periods (interval modis-start date))))