(ns forma.date-time-test
  (:use [forma.date-time] :reload)
  (:use midje.sweet
        [clj-time.core :only (date-time month interval)]))

(facts
 "Day conversions."
 (let [t1 (date-time 2005 01 01)
       mk-interval (fn [t2] (interval t1 t2))]
   (in-days (mk-interval (date-time 2005 1 28))) => 27
   (in-days (mk-interval (date-time 2005 2 24))) => 54
   (in-days (mk-interval (date-time 2008 9 12))) => 1350))

(facts
 "Julian date examples."
 (julian (date-time 2005 1 12)) => 11
 (julian (date-time 2005 4 29)) => 118
 (julian (date-time 2005 12 31)) => 364)

(facts
 "Julian date index should respect leap years."
 (julian (date-time 2008 3 1)) => 60
 (julian (date-time 2009 3 1)) => 59)

(facts
 "16 and 8 day periods per year, and 1 month periods per year."
 (per-year julian 16) => 23
 (per-year julian 8) => 46
 (per-year month 1) => 12)

(fact (periodize "32" (date-time 2005 12 04)) => 431)

(facts
 "Date conversion in various temporal resolutions."
 (datetime->period "32" "2005-12-04") => 431
 (datetime->period "32" "2006-01-01") => 432
 (datetime->period "16" "2005-12-04") => 826
 (datetime->period "16" "2003-04-22") => 765
 (datetime->period "16" "2003-04-23") => 766
 (datetime->period "8" "2003-04-12") => 1530)
