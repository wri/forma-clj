(ns forma.trends.stretch
  (:use [forma.matrix.utils :only (coll-avg)])
  (:require [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.thrift :as thrift]))

(defn shift-periods-target-res
  "Shift original ts start/end periods to appropriate values for target res"
  [base-res target-res start-idx end-idx]
  (map (partial date/shift-resolution base-res target-res)
       [start-idx (inc end-idx)]))

(defn expand-to-days
  "Expand timeseries to daily timeseries"
  [start-idx series pd base-res offset]
  (->> (map-indexed #(vector (+ % start-idx) %2) series)
       (mapcat (fn [[pd val]]
                 (repeat (date/period-span base-res pd) val)) )
       (drop offset)))

(defn ts-expander
  "timeseries is a TimeSeries.

  Expand a timeseries from lower resolution to higher resolution, by
   expanding the original timeseries to a daily timeseries, then
   consuming it at the new resolution.

   The final argument must be a TimeSeries object as defined in
   forma.thrift."
  [base-res target-res timeseries]
  (let [[start-idx end-idx series-value] (thrift/unpack timeseries)
        series (thrift/unpack series-value)
        [beg end] (shift-periods-target-res base-res target-res start-idx end-idx)
        offset (date/date-offset target-res beg base-res start-idx)]
    (loop [[pd & more :as periods] (range beg end)
           day-seq (expand-to-days start-idx series pd base-res offset)
           result (transient [])]
      (if (or (empty? periods) (empty? day-seq))
        (let [res (persistent! result)]
          (thrift/TimeSeries* (long beg) (long (dec (+ beg (count res)))) res))
        (let [num-days (date/period-span target-res pd)]
          (recur more
                 (drop num-days day-seq)
                 (conj! result (coll-avg (take num-days day-seq)))))))))
