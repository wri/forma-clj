;; This namespace allows for conversion of dates into integer time
;; periods, as measured from some reference date. This allows for
;; proper temporal comparison of two unrelated datasets.

(ns forma.date-time
  (:use [clj-time.core :only (date-time month year)]
        [clj-time.format :only (unparse formatters)]
        [clojure.string :only (split)]
        [clojure.contrib.math :only (ceil)])
  (:require [clj-time.core :as time]
            [clj-time.format :as f]))

;; ### Reference Time
;;
;; In developing the time period conversion functions, we noticed that
;; as long as the time period remained consistent from dataset to
;; dataset, the ability to 0-index each dataset became irrelevant.
;; Our choice of the [Unix epoch](http://goo.gl/KyuLr) as a common
;; reference date ensures that, regardless of the date on which the
;; specific MODIS product became active, datasets at the same temporal
;; resolution will match up.

;; ## Date -> Time Period Conversion
;;
;; For NASA's composite products, MODIS data is composited into either
;; monthly, 16-day, or 8-day intervals. Each of these scales begins
;; counting on January 1st. Monthly datasets begin counting on the
;; first of each month, while the others mark off blocks of 16 or 8
;; julian days from the beginning of the year.
;;
;; (It's important to note that time periods are NOT measured from the
;; activation date of a specific product. The first available NDVI
;; 16-day composite, for example, has a beginning date of February
;; 18th 2001, or Julian day 49, the 1st day of the 4th period as
;; measured from January 1st. With our reference of January 1st, This
;; dataset will receive an index of 3, and will match up with any
;; other data that falls within that 16-day period. This provides
;; further validation for our choice of January 1st, 2000 as a
;; consistent reference point.
;;
;; Additionally, it should be noted that the final dataset is never
;; full; the 23rd 16-day dataset of the year holds data for either 13
;; or 14 days, depending on leap year.)
;;
;; So! To construct a date, the user supplies a number of date
;; "pieces", ranging from year down to microsecond. To compare two
;; times on julian day, we use the clj-time library to calculate the
;; interval between a given date and January 1st of the year in which
;; the date occurs.

(defn in-days
  "Returns the number of days spanned by the given time interval."
  [interval]
  (-> interval time/in-minutes (/ 60) (/ 24) int))

(defn julian
  "Returns the julian day index of a given date."
  [a]
  (in-days
   (time/interval (date-time (year a))
             a)))

;; the julian function complements the other "date-piece" functions
;; supplied by the clj-time library; day, month, year, and the others
;; each allow for extraction of that particular element of a date. We
;; define the delta function to allow us to take the difference of a
;; specific date-piece between two dates.

(defn delta [f a b] (- (f b) (f a)))

;; For example,
;;
;;     (delta julian a b)
;;
;; returns the difference between the julian day values of the two
;; supplied dates. (Note that `(partial delta julian)` ignores the
;; year value of each of the dates. That function returns the same
;; value for a = Jan 1st, 2000, b = Feb 25th, 2000 and b = Feb 25th,
;; 2002.)

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
  (let [[years units] (map #(delta % start end) [time/year unit])]
    (+ (* years (per-year unit span))
       (quot units span))))

;; The following partial functions take care of the issue with delta
;; above, by calculating the span in periods across years. (Note that
;; these functions only work for MODIS products using regular
;; production. All terra products meet this restriction -- the aqua
;; products use phased production with 16- and 8-day temporal
;; resolution, so time periods begin on January 9th.)

(def months (partial delta-periods month 1))
(def sixteens (partial delta-periods julian 16))
(def eights (partial delta-periods julian 8))

(defn periodize
  "Converts the supplied `org.joda.time.DateTime` object into a
reference time interval at the supplied temporal
resolution. `DateTime` objects can be created with `clj-time`'s
`date-time` function."
  [temporal-res date]
  (let [period-func (case temporal-res
                          "32" months
                          "16" sixteens
                          "8" eights)]
    (period-func (time/epoch) date)))

(defn datetime->period
  "Converts a formatted datestring, into an integer time period at the
  supplied temporal resolution. The default format is
  `:year-month-day`, or `YYYY-MM-DD`; additional datestrings can by
  viewed with `(clj-time.format/show-formatters)`."
  ([res datestring]
     (datetime->period res datestring :year-month-day))
  ([res datestring format]
     (let [date (f/parse (f/formatters format)
                         datestring)]
       (periodize res date))))

(defn current-period
  "Returns the current time period for the supplied resolution. For
  example:

    (current-period \"32\")
    => 495 ;; (on April 27, 2011, this function's birthday!)"
  [res]
  (periodize res (time/now)))

;; ### Jobtag
;;
;; TODO: -- more information on what's going on here.
(defn jobtag
  "Generates a unique tag for a job, based on the current time."
  []
  (unparse (formatters :basic-date-time-no-ms)
           (time/now)))

;; TODO: Add docstrings
(defn msecs-from-epoch
  "Docstring."
  [date]
  (time/in-msecs
   (time/interval (time/epoch) date)))

(defn msec-range
  "Docstring."
  [start end]
  (let [total-months (inc (delta-periods month 1 start end))]
    (for [month-offset (range total-months)]
      (msecs-from-epoch (time/plus start
                                   (time/months month-offset))))))
