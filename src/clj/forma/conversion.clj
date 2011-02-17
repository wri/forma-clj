;; TODO -- what's the point of this file? We allow someone to convert
;; a date into either 16 day, 8 day, or monthly periods, as defined by
;; MODIS.
;; TODO -- should we change this so we can pass the function name
;; directly? The point of this is that we need some way to deal with
;; 1000m, 16 day data. Make a note in tracker to deal with this.

(ns forma.conversion
  (:use (clj-time [core :only (date-time year month in-minutes interval)])
        (clojure.contrib [math :only (ceil)])))

;; In developing the time period conversion functions, we noticed that
;; as long as the time period remained consistent from dataset to
;; dataset, the actual reference point, the zero-month, became
;; irrelevant. We chose the year 2000 because the MODIS system
;; recorded its earliest data in February of that year. MODIS indexing
;; will begin at 1, while NOAA PREC/L rainfall data, for example, will
;; begin at 0. For the purposes of our algorithm, we'll drop any
;; indices that hang off the ends, when the datasets are joined. (The
;; rain data's first month will be hacked off, as will later NDVI
;; months, if they don't match up with some index of rain data.)
;;
;; We keep this reference point throughout calculations of 16 day time
;; periods as well, as NASA's 16 day collection can be found at 16 day
;; offsets through the year. The final 16 day set is never full,
;; though its day count is either 13 or 14, depending on leap year status.

(def ref-date (date-time 2000))

(defn in-days
  "Returns the number of days spanned by the given interval."
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