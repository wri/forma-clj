(ns forma.trends.stretch-test
  (:use forma.trends.stretch
        midje.sweet)
  (:require [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.trends.data :as data]))

(fact
  (let [{:keys [start-idx end-idx]} (schema/timeseries-value 384 [1 2 3])]
    (shift-periods-target-res "32" "16" start-idx end-idx) => [736 741]))


(fact
  "Expect to see values repeated the correct number of days given 16-day dataset."
  (expand-to-days 384 [1 2 3] 384 1 "32" 0) => [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3])

(fact
  "Expect to see values repeated the correct number of days given a monthly dataset (e.g. 31 for January, 28 for non-leap year February."
  (expand-to-days 384 [1 2 3] 384 1 "1" 0) => [1 2 3])

(fact
  "Expect to see values repeated the correct number of days given a monthly dataset (e.g. 31 for January, 28 for non-leap year February."
  (expand-to-days 736 [1 2 3] 736 1 "16" 0) => (concat (repeat 16 1) (repeat 16 2) (repeat 16 3)))

(fact
  "Starting with January 2002 (non-leap year), period 384, a 3-period \"32\"-day timeseries. For a 32-day ts [1 2 3] for January-March, we expect to see five periods, through mid-March. The next period, which would run past the end of March, requires having the value for April. Since we only have through March, that second period in March is not generated."
  (let [ts (schema/timeseries-value 384 [1 2 3])
        output {:start-idx 736 :end-idx 740 :series [1.0 1.0625 2.0 2.3125 3.0]}]
    (ts-expander "32" "16" ts) => output))

(fact
  
  (let [periods-32 12
        periods-16 23
        ts (schema/timeseries-value 360 (take periods-32 data/ndvi))]
    (count (:series (ts-expander "32" "16" ts))) => periods-16))

(tabular
 (fact
   "A monthly timeseries of identical values Jan-Dec, followed by one additional, different value for the following January, should produce a 16-day timeseries of 23 periods of identical values for the year, plus one period, with a value from the 2nd January.

Note that this one additional period only covers the first half of January. The ts value for the second period in Janaury cannot be expanded until you've got data for February, since the second 16-day period runs through Feb. 1 (Jan. 17 + 16 = Feb. 1)

We'll add a few additional values to make sure extending beyond January works.

Expected outcome for adding twos to the end of the timeseries in a new year:

0 0 => all ones through end of December
1 1 => all ones then twos through first period of January
2 3 => all ones then twos through mid-February
3 5 => all ones then twos through mid-Marchoutput through mid-March
12 23 => all ones then twos through end of second December

This highlights that the expansion works for 32- to 16-day resolution except at the end of the year.

"
   (let [series (concat (repeat 12 1) (repeat ?extra-32 2))
         ts (schema/timeseries-value 360 series)
         expanded-ts (:series (ts-expander "32" "16" ts))
         expected-ts (map float (concat (repeat 23 1) (repeat ?extra-16 2)))]
     expanded-ts => expected-ts))
 ?extra-32 ?extra-16
 0          0
 1          1
 2          3
 3          5
 12         23)

(tabular
 (fact
   "Expanding a year-long monthly timeseries (12 periods) should yield a year's worth of 16-day periods (23). Here we simplify by using the same value for all periods, so `?periods-target-res` is just the number of periods we should see, ignoring the value for those periods.

The correct number of periods for 16- and 8-day timeseries is given by the number of directories in ftp://e4ftl01p.cr.usgs.gov/MOLT/MOD13A1.005 for the range of interest.

Period 360 is 2000-01-01 at monthly resolution
Period 736 is 2000-01-01 at 16-day resolution

The problem at present: The output timeseries seems to be too short by 1-2 periods. So even if we have monthly data for December (starting on 12/1/2011), the output will only run through 11/16/2011 because the 16-day periods are bounded by 12/1/2011 rather than 12/31/2011."

   (let [series (repeat ?periods-base-res 1)
         ts (schema/timeseries-value ?start-idx series)
         expected-count (count (:series (ts-expander ?base-res ?target-res ts)))
         ]
     expected-count => ?periods-target-res))
 ?base-res ?target-res ?periods-base-res ?periods-target-res ?start-idx
 "32"          "16"            1                1               360
 "32"          "16"            2                3               360
 "32"          "16"            3                5               360
 "32"          "16"            9                17              360
 "32"          "16"            10               19              360
 "32"          "16"            11               20              360 
 "32"          "16"            12               23              360
 "32"          "16"            13               24              360
 "32"          "16"            14               26              360
 "32"          "16"            24               46              360
 "32"          "16"            25               47              360
 "32"          "16"            120              230             360)

(tabular
 (future-fact
   "Same as above, but with higher target resolution"

   (let [series (repeat ?periods-base-res 1)
         ts (schema/timeseries-value ?start-idx series)
         expected-count (count (:series (ts-expander ?base-res ?target-res ts)))
         ]
     expected-count => ?periods-target-res))
 ?base-res ?target-res ?periods-base-res ?periods-target-res ?start-idx
 
 "32"          "8"             1                3               360
 "32"          "8"             2                7               360
 "32"          "8"             3                11              360
 "32"          "8"             12               45              360

 "16"          "8"             1                2               736
 "16"          "8"             2                4               736
 "16"          "8"             3                6               736
 "16"          "8"             23               45              736)

 
(tabular
 (future-fact
  "Double-check that a year-long timeseries extended by an extra few months will still have the correct. The values are all 1 for the first year, then 2 for the 'extra' periods"
   (let [series (concat (repeat 12 1) (repeat ?extra-32 2))
         ts (schema/timeseries-value 360 series)
         expanded-ts (:series (ts-expander "32" "8" ts))
         expected-ts (map float (concat (repeat 45 1) (repeat ?extra-8 2)))]
     expanded-ts => expected-ts))
 ?extra-32 ?extra-8
 0          0
 1          3
 2          7
 3          11
 12         45)

