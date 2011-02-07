;; This is where we'll be implementing OLS, using either infer or
;; incanter on the resulting matrices. Again, we should make sure that
;; we're operating on vectors, not seqs. (Turn a seq into a vector be
;; calling vec on it.)

(ns forma.trends
  (:use cascalog.api))

(defbufferop
  ^{:doc "Takes in a number of vectors, pre-sorted by time period,
representing the same MODIS chunk within a dataset, and aggregates
them by building a time series vector of vectors. Entering chunks
should be sorted in descending order."}
  timeseries [chunks]
  [(apply conj [] chunks)])