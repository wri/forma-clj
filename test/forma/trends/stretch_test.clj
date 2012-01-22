(ns forma.trends.stretch-test
  (:use forma.trends.stretch
        midje.sweet)
  (:require [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.trends.data :as data]))

(fact
  (let [ts (schema/timeseries-value 384 [1 2 3])]
    (shift-periods-target-res "32" "16" (:start-idx ts) (:end-idx ts)) => [736 739]))

(fact
  "Expect to see values repeated the correct number of days given a monthly dataset (e.g. 31 for January, 28 for non-leap year February."
  (expand-to-days 384 [1 2 3] 384 1 "32" 0) => [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3])

(fact
  "Starting with January 2002 (non-leap year), period 384, a 3-period \"32\"-day timeseries."
  (let [ts (schema/timeseries-value 384 [1 2 3])
        output {:start-idx 736 :end-idx 739 :series [1.0 1.0625 2.0 2.3125]}]
    (ts-expander "32" "16" ts) => output))

(fact
  "Expanding a year-long monthly timeseries (12 periods) should yield a year's worth of 16-day periods (23)."
  (let [periods-32 12 ;; number of periods, 32-day res
        periods-16 23 ;; expected number of periods, 16-day res
        ts (schema/timeseries-value 360 (take periods-32 data/ndvi))]
    (count (:series (ts-expander "32" "16" ts))) => periods-16))