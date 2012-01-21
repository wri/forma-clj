(ns forma.trends.stretch
  (:use [forma.matrix.utils :only (coll-avg)])
  (:require [forma.date-time :as date]
            [forma.schema :as schema]))



(defn beg-end [base-res target-res start-idx end-idx]
  (map (partial date/shift-resolution base-res target-res)
                       [start-idx end-idx]))

(defn get-day-seq
  [start-idx series pd val base-res offset]
  (->> (map-indexed #(vector (+ % start-idx) %2) series)
       (mapcat (fn [[pd val]]
                 (repeat (date/period-span base-res pd) val)) )
       (drop offset)))

(defn ts-expander
  "`series` must be a sequence of 2-tuples of the form `[period, val]`."
  [base-res target-res tseries]
  (let [{:keys [start-idx end-idx series]} tseries
        [beg end] (beg-end base-res target-res start-idx end-idx)
        offset (date/date-offset target-res beg base-res start-idx)]
    (loop [[pd & more :as periods] (range beg (inc end))
           day-seq (get-day-seq start-idx series pd val base-res offset)
           result []]
      (if (empty? periods)
        (schema/timeseries-value beg result)
        (let [num-days (date/period-span target-res pd)]
          (println (count day-seq))
          (recur more
                 (drop num-days day-seq)
                 (conj result (coll-avg (take num-days day-seq)))))))))