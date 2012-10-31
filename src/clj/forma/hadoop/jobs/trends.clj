(ns forma.hadoop.jobs.trends
  (:use cascalog.api
        [midje sweet cascalog]
        [forma.matrix.utils :only (sparse-expander)])
  (:require [forma.date-time :as date]))

(defn sorted-ts
  "Accepts a map with date keys and time series values, and returns a
  vector with the values appropriately sorted.

  Example:
    (sorted-ts {:2005-12-31 3 :2006-08-21 1}) => (3 1)"
  [m]
  [(vals (into (sorted-map) m))])

(defn key->period
  [t-res k]
  (date/datetime->period t-res (name k)))

(defn period->key
  [t-res pd]
  (keyword (date/period->datetime t-res pd)))

(defn key-span
  "Returns a list of date keys, beginning and end date inclusive.  The
  first and last keys represent the periods that start just before the
  supplied boundary dates.

  Example:
    (key-span :2005-12-31 :2006-01-18 \"16\")
    => (:2005-12-19 :2006-01-01 :2006-01-17)"
  [init-key end-key t-res]
  (let [init-idx (key->period t-res init-key)
        end-idx  (inc (key->period t-res end-key))]
    (map (partial period->key t-res)
         (range init-idx end-idx))))

(defn same-len? [coll1 coll2]
  (= (count coll1) (count coll2)))

(defn all-unique?
  [coll]
  (same-len? coll (set coll)))

(defn vec->ordered-map
  "Accepts a collection and a key that indicates the first element in
  the collection, and a temporal resolution."
  [init-key t-res coll]
  (let [init (key->period t-res init-key)
        end-key  (period->key t-res (+ init (count coll)))]
    (zipmap (key-span init-key end-key t-res) coll)))

(defn update-map []
  (let [old-src [[28 8 0 0 {:2005-12-19 0 :2006-01-01 1 :2006-01-17 2}]]
        new-src [[28 8 0 0 {:2006-02-02 3 :2006-02-18 4}]]]
    (<- [?h ?v ?s ?l ?updated-map]
        (old-src ?h ?v ?s ?l ?old-map)
        (new-src ?h ?v ?s ?l ?new-map)
        (merge ?old-map ?new-map :> ?updated-map))))
