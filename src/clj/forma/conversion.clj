;; This namespace allows for conversion of dates into integer time
;; periods, as measured from some reference date. This allows for
;; proper temporal comparison of two unrelated datasets.

(ns forma.conversion
  (:use (clj-time [core :only (date-time year month in-minutes interval)])
        (clojure.contrib [math :only (ceil)])))

;; In developing the time period conversion functions, we noticed that
;; as long as the time period remained consistent from dataset to
;; dataset, the ability to 0-index became irrelevant.  Our choice of
;; January 1st, 2000 ensures that, regardless of the date on which the
;; specific MODIS product became active, datasets at the same temporal
;; resolution will match up. With that in mind, we chose January 1st
;; 2000.

(def ref-date (date-time 2000))

;; For NASA's composite products, MODIS data is composited into either
;; monthly, 16-day, or 8-day intervals. Each of these scales begins
;; counting on January 1st. Monthly datasets begin counting on the
;; first of each month, while the others mark off blocks of 16 or 8
;; julian days from the beginning of the year.
;;
;;(It's important to note that time periods are NOT measured from the
;; activation date of a specific product. The first available NDVI
;; 16-day composite, for example, has a beginning date of February
;; 18th 2001, or Julian day 49, the 1st day of the 4th period as
;; measured from January 1st. With our reference of January 1st, This
;; dataset will receive an index of 3, and will match up with any
;; other data that falls within that 16-day period. This provides
;; further validation for our choice of January 1st, 2000 as a
;; consistent reference point.
;;
;; Additionally it should be noted that the final dataset is never
;; full; the 23rd 16-day dataset of the year holds data for either 13
;; or 14 days, depending on leap year.)
;;
;; TODO COMPLETE FOM HERE ON OUT
;; TODO -- what's the point of this file? We allow someone to convert
;; a date into either 16 day, 8 day, or monthly periods, as defined by
;; MODIS.
;; TODO -- should we change this so we can pass the function name
;; directly? The point of this is that we need some way to deal with
;; 1000m, 16 day data. Make a note in tracker to deal with this.

(defn in-days
  "Returns the number of days spanned by the given time interval."
  [interval]
  (let [mins (in-minutes interval)
        hours (/ mins 60)
        days (/ hours 24)]
    (int days)))

(defn julian
  "Returns the julian day index of a given date."
  [a]
  (in-days
   (interval (date-time (year a))
             a)))

(defn delta [f a b] (- (f b) (f a)))

(defn per-year
  "Calculates how many periods of the given span of supplied units can
  be found in a year. Includes the final period, even if that period
  isn't full."
  [unit span]
  (let [m {julian 365 month 12}]
    (ceil (/ (m unit) span))))

(defn delta-periods
  "Calculates the difference between the supplied start and end dates
  in span-sized groups of unit (months or julians). [unit span] could be
  [julian 16], for example."
  [unit span start end]
  (let [[years units] (map #(delta % start end) [year unit])]
    (+ (* years (per-year unit span))
       (quot units span))))

(def months (partial delta-periods month 1))
(def sixteens (partial delta-periods julian 16))
(def eights (partial delta-periods julian 8))

(def period-func
  {"1000" months
   "500" sixteens
   "250" sixteens})

(defn datetime->period
  "Converts a given set of date pieces into a reference time interval
at the same temporal resolution as a MODIS product at the supplied
resolution. Input can be any number of pieces of a date, from greatest
to least significance. See clj-time's date-time function for more
information."
  [res & int-pieces]
  (let [date (apply date-time int-pieces)]
    ((period-func res) ref-date date)))