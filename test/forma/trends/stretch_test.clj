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
  "Expect to see values repeated the correct number of days given
  16-day dataset."
  (expand-to-days 384 [1 2 3] 384 1 "32" 0) => (concat (repeat 31 1)
                                                       (repeat 28 2)
                                                       (repeat 31 3)))

(fact
  "Expect to see values repeated the correct number of days given a
monthly dataset (e.g. 31 for January, 28 for non-leap year February."
  (expand-to-days 0 [1 2 3] 384 1 "1" 0) => [1 2 3])

(fact
  "Expect to see values repeated the correct number of days given a
  monthly dataset (e.g. 31 for January, 28 for non-leap year
  February."
  (expand-to-days 736 [1 2 3] 736 1 "16" 0) => (concat (repeat 16 1)
                                                       (repeat 16 2)
                                                       (repeat 16 3)))

(fact
  "Starting with January 2002 (non-leap year), period 384, a 3-period
  \"32\"-day timeseries. For a 32-day ts [1 2 3] for January-March, we
  expect to see five periods, through mid-March. The next period,
  which would run past the end of March, requires having the value for
  April. Since we only have through March, that second period in March
  is not generated."
  (let [ts (schema/timeseries-value 384 [1 2 3])
        output {:start-idx 736
                :end-idx 740
                :series [1.0 1.0625 2.0 2.3125 3.0]}]
    (ts-expander "32" "16" ts) => output))

(defn get-periods-per-year
  "Returns the number of periods in a year at the resolution of interest."
  [k]
  ((keyword k) {:32 12 :16 23 :8 46}))

(defn mk-ts
  "Create a timeseries based on the number of periods in a year, the number of periods to consume, and the starting index. Values are repeated based on `periods-per-year`, and increment with the year index `(range years)."
  [periods-per-year amount start-idx]
  (let [years 10]
    (schema/timeseries-value start-idx
                             (map float (take amount
                                              (reduce concat
                                                      (map
                                                       (partial repeat periods-per-year)
                                                       (range years))))))))

(tabular
 (fact
   "Expanding a year-long monthly timeseries (12 periods) should yield a year's worth of 16-day (23) or 8-day (46) periods. Since we established above that the moving average works correctly, we are focusing here on ensuring that the number of periods is correct, and that the end of the year is accounted for correctly. Sixteen- and 8-day timeseries do not fit evenly into a year, so the final period of each year will always be shorter than a normal period by several days.

Here, our timeseries values are repeated in a given year, as described in `mk-ts`.

The number of periods for 16- and 8-day timeseries should be given by
the number of directories in
ftp://e4ftl01p.cr.usgs.gov/MOLT/MOD13A1.005 for the range of
interest, but there appears to be some inconsistency ...

For reference:
Period 360 is 2000-01-01 at monthly resolution
Period 736 is 2000-01-01 at 16-day resolution
Period 1380 is 2000-01 at 8-day resolution"
   (let [target-start-idx (date/shift-resolution ?base-res ?target-res ?start-idx)
         base-ts (mk-ts
                      (get-periods-per-year ?base-res)
                      ?base-length
                      ?start-idx)
         target-ts (mk-ts
                        (get-periods-per-year ?target-res)
                        ?target-length
                        target-start-idx)]     
     (ts-expander ?base-res ?target-res base-ts) => target-ts))
 ?base-length ?target-length  ?base-res ?target-res ?start-idx
 1                1              "32"       "16"        360
 2                3              "32"       "16"        360
 3                5              "32"       "16"        360
 10               19             "32"       "16"        360
 11               20             "32"       "16"        360
 12               23             "32"       "16"        360
 24               46             "32"       "16"        360
 36               69             "32"       "16"        360
 37               70             "32"       "16"        360
 120              230            "32"       "16"        360
 
 1                3              "32"       "8"         360
 2                7              "32"       "8"         360
 12               46             "32"       "8"         360
 24               92             "32"       "8"         360
 25               95             "32"       "8"         360
 36               138            "32"       "8"         360
 
 1                2              "16"       "8"         690
 2                4              "16"       "8"         690
 23               46             "16"       "8"         690
 24               48             "16"       "8"         690
 25               50             "16"       "8"         690)
