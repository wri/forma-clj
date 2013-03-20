(ns forma.date-time-test
  (:use [forma.date-time] :reload)
  (:use midje.sweet
        [clj-time.core :only (now date-time month interval)]))

(facts "Date parsing and conversion tests."
  (parse "2011-06-23" :year-month-day) => (date-time 2011 6 23)
  (parse "20110623" :basic-date) => (date-time 2011 6 23)
  (convert "2011-06-23" :year-month-day :basic-date) => "20110623")

(facts "within-dates? test."
  (within-dates? "2005-12-01" "2011-01-02" "2011-01-01") => true
  (within-dates? "20051201" "20110101" "20110102" :format :basic-date) => false

  "within-dates? can't accept datestrings that don't match the default
or supplied formats."
  (within-dates? "2005" "2011" "2010") => (throws IllegalArgumentException))

(tabular
 (fact "Ordinal date examples."
   (ordinal (apply date-time ?pieces)) => ?day)
 ?pieces ?day
 [2005 1 12] 11
 [2005 4 29] 118
 [2005 12 31] 364)

(tabular
 (fact "Ordinal date index should respect leap years."
   (ordinal (apply date-time ?pieces)) => ?day)
 ?pieces ?day
 [2008 3 1] 60
 [2009 3 1] 59)

(facts "Delta testing."  
  (delta #(* % %) 3 4) => 7
  (delta ordinal
         (date-time 2005 01 12)
         (date-time 2005 01 15)) => 3)

(tabular
 (fact "16 and 8 day periods per year, and 1 month periods per year."
   (per-year ?unit ?length) => ?days-per)
?unit    ?length ?days-per
ordinal  16      23
ordinal  8       46
month    1       12)

(fact (periodize "32" (date-time 2005 12 04)) => 431)

(tabular
 (facts "Date conversion, forward and backward, in various temporal
 resolutions."
   (datetime->period ?res ?date) => ?period
   (period->datetime ?res ?period) => (beginning ?res ?date))
 ?res ?date        ?period
 "32" "2005-12-04" 431
 "32" "2006-01-01" 432
 "16" "2005-12-04" 826
 "16" "2003-04-22" 765
 "16" "2003-04-23" 766
 "16" "2003-12-31" 781
 "8"  "2003-04-12" 1530
 "8"  "2003-12-31" 1563)

(facts "Beginning tests!"
  (beginning "16" "2005-12-31") => "2005-12-19"
  (beginning "32" "2005-12-31") => "2005-12-01"
  (beginning "32" "2011-06-23T22" :date-hour) => "2011-06-01T00")

(facts "diff-in-days tests"
  "Dates properly span years..."
  (diff-in-days "2011-12-30" "2012-01-02") => 3

  "And months."
  (diff-in-days "2011-11-01" "2012-01-02") => 62

  "The two dates must be provided in increasing order."
  (diff-in-days "2012-01-02" "2011-12-30") => (throws IllegalArgumentException))

(facts "date-offset tests"
  (date-offset "16" 1 "32" 1) => 15)

(facts "period-span tests."
  "December of the second year."
  (period-span "32" 12) => 31

  "Checking that we get proper month lengths for all of 1970 (first 12
   months since the epoch)."
  (for [month (range 12)]
    (period-span "32" month)) =>  [31 28 31 30 31 30 31 31 30 31 30 31])

(facts "shift-resolution tests"
  (shift-resolution "32" "16" 1) => 1
  (shift-resolution "32" "16" 10) => 19)

(facts "current-period tests."
  (current-period "32") => 431
  (current-period "16") => 825

  (against-background
    (now) => (date-time 2005 12 01 13 10 9 128)))

(fact "Relative period test."
  (relative-period "32" 391 ["2005-02-01" "2005-03-01"]) => [30 31])

(fact "Millisecond tests."
  (msecs-from-epoch (date-time 2002 12)) => 1038700800000
  (monthly-msec-range (date-time 2005 11)
                      (date-time 2005 12)) => [1130803200000 1133395200000])

(facts
  "Check that `date-str->vec-idx` outputs the correct index for a date"
  (date-str->vec-idx "16" "2000-01-01" [2 4 6] "2000-01-17") => 1
  (date-str->vec-idx "16" "2000-01-01" [1 2 3] "2001-01-01") => nil)

(facts
  "Check that `get-val-at-date` outputs the correct index for a date"
  (get-val-at-date "16" "2000-01-01" [2 4 6] "2000-01-17") => 4
  (get-val-at-date "16" "2000-01-01" [1 2 3] "2001-01-01") => nil
  (get-val-at-date "16" "2000-01-01" [2 4 6] "2005-01-01" :out-of-bounds-val 5) => 5
  (get-val-at-date "16" "2000-01-01" [2 4 6] "2005-01-01" :out-of-bounds-idx 0) => 2)

(tabular
 (fact "Check merge-ts."
   (merge-ts ?master ?new :update ?update)
   => ?result)
 ?master ?new ?update ?result

 ;; non-overlapping, :update true
 {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3}
 {:2012-02-18 4}
 true {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3 :2012-02-18 4}
 
 ;; non-overlapping, :update false
 {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3}
 {:2012-02-18 4}
 false {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3 :2012-02-18 4}
 
 ;; overlapping time series, :update true
 {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3}
 {:2012-02-02 4}
 true {:2012-01-01 1 :2012-01-17 2 :2012-02-02 4}
 
 ;; overlapping time series, :update false
 {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3}
 {:2012-02-02 -1}
 false (throws AssertionError)
 
 ;; overlapping time series, where new time series updates elements
 ;; in the middle of the master time series
 {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3 :2012-02-18 4}
 {:2012-01-17 -1 :2012-02-02 -1}
 true {:2012-01-01 1 :2012-01-17 -1 :2012-02-02 -1 :2012-02-18 4}

 ;; non-consecutive - hole in master series
 {:2012-01-01 1 :2012-01-17 2 :2013-01-01 3}
 {:2013-02-02 4}
 false {:2012-01-01 1 :2012-01-17 2 :2013-01-01 3 :2013-02-02 4}
 
 ;; non-consecutive - hole in new series
 {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3}
 {:2012-02-18 4 :2012-03-21 5}
 false {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3 :2012-02-18 4 :2012-03-21 5})

(comment
   (fact


   "Test merge-ts"
   ;; non-overlapping, sequential time series.
   (let [a {:start-idx 693 :resolution "16" :series (range 2)}
         b {:start-idx 695 :resolution "16" :series (range 2 4)}]
     (merge-ts a b)) => {:start-idx 693 :resolution "16" :series (range 4)}

     ;; overlapping time series, where overlap values are the same.
     ;; (let [a {:start-idx 693 :resolution "16" :series (range 2)}
     ;;       b {:start-idx 694 :resolution "16" :series (range 2 4)}]
     ;;   (merge-ts a b)) => {:start-idx 693 :resolution "16" :series (range 4)}

     ;; overlapping time series, :update flag not used
     (let [a {:start-idx 693 :resolution "16" :series (range 2)}
           b {:start-idx 694 :resolution "16" :series (range 5 8)}]
       (merge-ts a b) => (throws Exception))

     ;; overlapping time series, :update flag is used.
     (let [a {:start-idx 693 :resolution "16" :series (range 2)}
           b {:start-idx 694 :resolution "16" :series (range 5 8)}]
       (merge-ts a b :update true ) => {:start-idx 693 :resolution "16" :series [0 5 6 7]})

     ;; overlapping time series, where one time series updates only a few
     ;; elements in the middle of the original time series
     (let [a {:start-idx 693 :resolution "16" :series (range 5)}
           b {:start-idx 695 :resolution "16" :series [20]}]
       (merge-ts a b :update true ) => {:start-idx 693 :resolution "16" :series [0 1 20 3 4]})

     ;; non-sequential time series results in hole in output - no :nodata flag
     (let [a {:start-idx 693 :resolution "16" :series (range 2)}
           b {:start-idx 700 :resolution "16" :series (range 2 4)}]
       (merge-ts a b) => (throws Exception))

     ;; non-sequential time series results in hole in output - :nodata flag used
     (let [a {:start-idx 693 :resolution "16" :series (range 2)}
           b {:start-idx 700 :resolution "16" :series (range 2 4)}]
       (merge-ts a b :nodata -9999.0)
       => {:start-idx 693 :resolution "16" :series [0 1 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 2 3]})

     ;; using both flags is ok - example uses non-sequential time series
     (let [a {:start-idx 693 :resolution "16" :series (range 2)}
           b {:start-idx 700 :resolution "16" :series (range 2 4)}]
       (merge-ts a b :update true :nodata -9999.0)
       => {:start-idx 693 :resolution "16" :series [0 1 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0 2 3]})

     ;; using both flags is ok - example uses overlapping time series, and overlap
     ;; values are different
     (let [a {:start-idx 693 :resolution "16" :series (range 2)}
           b {:start-idx 694 :resolution "16" :series (range 5 8)}]
       (merge-ts a b :update true :nodata -9999.0)
       => {:start-idx 693 :resolution "16" :series [0 5 6 7]})))

(tabular
 (fact "Check ts-vec->ts-map"
   (ts-vec->ts-map ?date ?t-res ?coll) => ?result)
 ?date ?t-res ?coll ?result
 :2012-01-01 "16" [1 2 3] {:2012-01-01 1 :2012-01-17 2 :2012-02-02 3}
 :2012-01-01 "16" [3 2 1] {:2012-01-01 3 :2012-01-17 2 :2012-02-02 1})

(tabular
 (fact "Check ts-map->ts-vec"
   (ts-map->ts-vec ?t-res ?m ?nodata) => ?result)
 ?t-res ?m ?nodata ?result
 "16" {:2012-01-01 1 :2012-01-17 2} -9999.0 [1 2]
 "16" {:2012-01-01 1 :2012-02-02 2} -9999.0 [1 -9999.0 2]
 "32" {:2012-01-01 1 :2012-02-01 2} -9999.0 [1 2]
 "32" {:2012-01-01 1 :2012-03-01 2} -9999.0 [1 -9999.0 2])




