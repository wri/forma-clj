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
  (expand-to-days 384 [1 2 3] 384 1 "32" 0) => [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3])

(fact
  "Starting with January 2002 (non-leap year), period 384, a 3-period \"32\"-day timeseries."
  (let [ts (schema/timeseries-value 384 [1 2 3])
        output {:start-idx 736 :end-idx 739 :series [1.0 1.0625 2.0 2.3125]}]
    (ts-expander "32" "16" ts) => output))

(fact
  "Expand a longer timeseries - in this case one year of NDVI"
  (let [p32 12 ;; number of periods, 32-day res
        p16 22 ;; expected number of periods, 16-day res
        ts (schema/timeseries-value 360 (take p32 data/ndvi))
        out-ts (ts-expander "32" "16" ts)]
    (println (date/period->datetime "16" (:start-idx out-ts)))
    (println (date/period->datetime "16" (:end-idx out-ts)))
    (count (:series out-ts)) => p16))