(ns forma.hadoop.jobs.api-test
  "We’ll generate an API master dataset after each update. This
  dataset will be vertically partitioned into sequence files using a
  template tap as follows:

  /api/{iso}/{year}"
  (:use cascalog.api
        [midje sweet cascalog])
  (:use forma.hadoop.jobs.api :reload)
  (:require [cascalog.logic.ops :as c]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.utils :as u]))

(def est-map {:nodata -9999.0 :est-start "2005-12-19" :t-res "16"})

(def sample-forma-data
  [["500" 28 8 0 0 827 [0.5 0.49 0.55 0.54 0.60 0.59 0.5] 88500]
   ["500" 28 8 0 1 827 [0. 0. 0. 0. 0. 0. 0.] 88500]
   ["500" 28 8 0 2 827 [0. 0. 0.2 0.4 0.3 0. 0.] 88500]
   ["500" 28 8 0 3 827 [0. 0. 0. 0. 0.19 0.4 0.99] 88500]
   ["500" 28 8 0 4 827 [0. 0. 0. 0. 0. 0. 0.99] 88500]])

(def sample-static-data
  [["500" 28 8 0 0 88114]])

(fact "Test `wide->long`."
  (let [start-idx 827
        src [[1 [1 2 3]]]]
    (<- [?id ?pd ?val]
        (src ?id ?series)
        (wide->long start-idx ?series :> ?pd ?val)))
  => (produces [[1 827 1] [1 828 2] [1 829 3]]))

(fact "Test `get-hit-val`."
  (get-hit-val 20 [1 2 20 40]) => 20
  (get-hit-val 20 [1 2 25 40]) => 25)

(fact "Test `get-hit-period`."
  (get-hit-period 0 20 [1 2 3 20]) => 3
  (get-hit-period 827 20 [1 2 3 20]) => 830)

(fact "Test `latest-hit?`."
  (latest-hit? 20 [1 2 20]) => true ;; past thresh in latest period
  (latest-hit? 20 [1 2 20 30]) => false ;; passed thresh before latest period
  (latest-hit? 50 [1 2 20 30]) => false
  (latest-hit? 50 [1 2 20 50]) => true
  (latest-hit? 50 [1 2 20 55]) => true)

(tabular
 (fact "Test `get-hit-period-and-value`."
   (get-hit-period-and-value 827 ?thresh ?series) => ?result)
 ?thresh ?series ?result
 20 [1 2 20] [829 20]
 50 [1 2 20] [nil nil])

(fact "Check `prob-series->long-ts`. Note that output probabilities
       are smoothed. The second test includes a threshold."
  (prob-series->long-ts est-map sample-forma-data)
  => (produces-some
      [[9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2005-12-19" "2005" 50]
       [9.99374999999999 101.542824160711 "IDN" "IDN" 88500 "2006-02-02" "2006" 0]])

  (prob-series->long-ts est-map sample-forma-data 50)
  => (produces-some
         [[9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2005-12-19" "2005" 50]
          [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-01-01" "2006" 50]
          [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-01-17" "2006" 51]
          [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-02-02" "2006" 53]
          [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-02-18" "2006" 56]
          [9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2006-03-06" "2006" 58]]))

(fact "Test `prob-series->first-hit"
  (prob-series->first-hit est-map sample-forma-data)
  => (produces
      [[9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2005-12-19" 50]
       [9.99374999999999 101.542824160711 "IDN" "IDN" 88500 "2005-12-19" 0]
       [9.98958333333332 101.54152320701927 "IDN" "IDN" 88500 "2005-12-19" 0]
       [9.985416666666667 101.54022282365065 "IDN" "IDN" 88500 "2005-12-19" 0]
       [9.981249999999996 101.53892301056953 "IDN" "IDN" 88500 "2005-12-19" 0]])
  (prob-series->first-hit est-map sample-forma-data 20)
  => (produces
      [[9.99791666666666 101.54412568476158 "IDN" "IDN" 88500 "2005-12-19" 50]
       [9.98958333333332 101.54152320701927 "IDN" "IDN" 88500 "2006-02-02" 20]
       [9.985416666666667 101.54022282365065 "IDN" "IDN" 88500 "2006-03-06"20]
       [9.981249999999996 101.53892301056953 "IDN" "IDN" 88500 "2006-03-22" 33]]))

(fact "Test `prob-series->latest`."
    (prob-series->latest est-map sample-forma-data 50)
  => (produces
      [[9.985416666666667 101.54022282365065 "IDN" "IDN" 88500 "2006-03-22" 53]])
  (prob-series->latest est-map sample-forma-data 20)
  => (produces
      [[9.981249999999996 101.53892301056953 "IDN" "IDN" 88500 "2006-03-22" 33]]))
