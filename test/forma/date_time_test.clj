(ns forma.date-time-test
  (:use [forma.date-time] :reload)
  (:use midje.sweet
        [clj-time.core :only (date-time month interval)]))

(tabular
 (fact "Day conversions."
   (let [t1 (date-time 2005 01 01)
         mk-interval (fn [t2] (interval t1 t2))]
     (in-days (mk-interval (apply date-time ?pieces))) => ?result))
 ?pieces ?result
 [2005 1 28] 27
 [2005 2 24] 54
 [2008 9 12] 1350)

(tabular
 (fact "Julian date examples."
   (julian (apply date-time ?pieces)) => ?day)
 ?pieces ?day
 [2005 1 12] 11
 [2005 4 29] 118
 [2005 12 31] 364)

(tabular
 (fact "Julian date index should respect leap years."
   (julian (apply date-time ?pieces)) => ?day)
 ?pieces ?day
 [2008 3 1] 60
 [2009 3 1] 59)

(tabular
 (fact "16 and 8 day periods per year, and 1 month periods per year."
   (per-year julian ?length ?days-per)
   ?length ?days-per
   16 23, 8 46, 1 12))

(fact (periodize "32" (date-time 2005 12 04)) => 431)

(tabular
 (fact "Date conversion in various temporal resolutions."
   (datetime->period ?res ?date) => ?period)
 ?res ?date        ?period
 "32" "2005-12-04" 431
 "32" "2006-01-01" 432
 "16" "2005-12-04" 826
 "16" "2003-04-22" 765
 "16" "2003-04-23" 766
 "8"  "2003-04-12" 1530)
