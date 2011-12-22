(ns forma.trends.stretch
  (:use [forma.matrix.utils :only (coll-avg)])
  (:require [forma.hadoop.io :as io]
            [forma.date-time :as date]))

(defn ts-expander
  "`xs` must be a sequence of 2-tuples of the form `[period, val]`."
  [base-res target-res tseries]
  (let [[beg-idx end-idx xs] ((juxt #(.getStartIdx %)
                                    #(.getEndIdx %)
                                    io/get-vals)
                              tseries)
        [beg end] (map (partial date/shift-resolution base-res target-res)
                       [beg-idx end-idx])
        offset (date/date-offset target-res beg base-res beg-idx)]
    (loop [[pd & more :as periods] (range beg (inc end))
           day-seq (->> (map-indexed #(vector (+ % beg) %2) xs)
                        (mapcat (fn [[pd val]]
                                  (repeat (date/period-span base-res pd) val)))
                        (drop offset))
           result []]
      (if (empty? periods)
        (io/timeseries-value beg result)
        (let [num-days (date/period-span target-res pd)]
          (recur more
                 (drop num-days day-seq)
                 (conj result (coll-avg (take num-days day-seq)))))))))
