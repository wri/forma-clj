(ns forma.conversion
  (:use (clj-time [core :only (date-time year month day)])
        (clojure.contrib [math :only (ceil)])))



;; We have to pick some sort of reference starting point, and the
;; first of january is as good as any!
;; TODO -- flesh this out. Why the first?
(def ref-date (date-time 2000))

(defn delta [f a b] (- (f b) (f a)))

(defn per-year
  "Calculates how many periods of the given span of supplied units can
  be found in a year. Includes the final period, even if that period
  isn't full.

  Example usage:
  (per-year day 16)
  => 23"
  [unit span]
  (ceil
   (/ (condp = unit
          day 365
          month 12
          year 1)
      span)))

(defn delta-periods
  "TODO --write doc string."
  [unit span start end]
  (let [[years units] (map #(delta % start end) [year unit])]
    (+ (* years (per-year unit span))
       (quot units span))))

(def months (partial delta-periods month 1))
(def sixteens (partial delta-periods day 16))
(def eights (partial delta-periods day 8))

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