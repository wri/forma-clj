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
  "Starting with January 2002 (non-leap year), period 384, a 3-period \"32\"-day timeseries. For a 32-day ts [1 2 3] for January-March, we expect to see five periods, through mid-March. The next period, which would run past the end of March, requires having the value for April. Since we only have through March, that second period in March is not generated."
  (let [ts (schema/timeseries-value 384 [1 2 3])
        output {:start-idx 736 :end-idx 740 :series [1.0 1.0625 2.0 2.3125 3.0]}]
    (ts-expander "32" "16" ts) => output))

(fact
  "Expanding a year-long monthly timeseries (12 periods) should yield a year's worth of 16-day periods (23)."
  (let [periods-32 12 ;; number of periods, 32-day res
        periods-16 23 ;; expected number of periods, 16-day res
        ts (schema/timeseries-value 360 (take periods-32 data/ndvi))]
    (count (:series (ts-expander "32" "16" ts))) => periods-16))

(tabular
 (fact
   "A monthly timeseries of identical values Jan-Dec, followed by one additional, different value for the following January, should produce a 16-day timeseries of 23 periods of identical values for the year, plus one period, with a value from the 2nd January.

Note that this one additional period only covers the first half of January. The ts value for the second period in Janaury cannot be expanded until you've got data for February, since the second 16-day period runs through Feb. 1 (Jan. 17 + 16 = Feb. 1)

We'll add a few additional values to make sure extending beyond January works.

0 0 => all ones through end of December
1 1 => all ones then twos through first period of January
2 3 => all ones then twos through mid-February
3 5 => all ones then twos through mid-Marchoutput through mid-March
12 23 => all ones then twos through end of second December

The problem at present: The output timeseries seems to be too short by 1-2 periods. So even if we have monthly data for December (starting on 12/1/2011), the output will only run through 11/16/2011 because the 16-day periods are bounded by 12/1/2011 rather than 12/31/2011.
"
   (let [series (concat (repeat 12 1) (repeat ?extra-32 2))
         ts (schema/timeseries-value 360 series)
         expanded-ts (:series (ts-expander "32" "16" ts))
         expected-ts (map float (concat (repeat 23 1) (repeat ?extra-16 2)))]
     expanded-ts => expected-ts))
 ?extra-32 ?extra-16
 0          0
 1          1 ;; outputs through first half of January
 2          3 ;; outputs through mid-January
 3          5 ;; outputs through 
 12         23)


