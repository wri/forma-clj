;; This is where we'll be implementing OLS regression.

(ns forma.trends
  (:use cascalog.api))

(defbufferop timeseries
  ^{:doc "Takes in a number of vectors, pre-sorted by time period,
representing the same MODIS chunk within a dataset, and aggregates
them by building a time series vector of vectors. Entering chunks
should be sorted in descending order."}
  [chunks]
  [(apply conj [] chunks)])
