(ns forma.trends.analysis
  (:use [forma.matrix.utils :only (variance-matrix coll-avg)]
        [forma.trends.filter :only (deseasonalize make-reliable)]
        [clojure.contrib.math :only (sqrt)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]))

;; The first few functions are more general, and could be pulled into,
;; say, the matrix operation namespace.  For now, we will leave them
;; here, until we can talk more about what goes into that namespace.

(defn time-trend-cofactor
  "construct a matrix of cofactors; first column is comprised of ones,
  second column is a range from 1 to [num-months].  The result is a
  [num-months]x2 incanter matrix."
  [num-months]
  (i/trans (i/bind-rows (repeat num-months 1)
                        (range 1 (inc num-months)))))

(defn singular?
  "Check to see if the supplied matrix `X` is singular. note that `X` has to
  be square, n x n, where n > 1."
  [X]
  (<= (i/det X) 0))

(defn ols-coefficient
  "extract OLS coefficient from a time-series."
  [ts]
  (let [ycol (i/trans [ts])
        X (time-trend-cofactor (count ts))]
    (i/sel (i/mmult (variance-matrix X)
                    (i/trans X)
                    ycol)
           1 0)))

(defn windowed-apply 
  "apply a function [f] to a window along a sequence [xs] of length [window]"
  [f window xs]
  (pmap f (partition window 1 xs)))

(defn make-monotonic
  "move through a collection `coll` picking up the min or max values, depending
  on the value of `comparator` which can be either `min` or `max`.  This is very
  similar, I think, to the `reduce` function, but with side effects that print
  into a vector."
  [comparator coll]
  (reduce (fn [acc val]
            (conj acc
                  (if (empty? acc)
                    val
                    (comparator val (last acc)))))
          []
          coll))

;; WHOOPBANG :: Collect the short-term drop associated with a time-series

;; The following is a routine, under the header WHOOPBANG (original name),
;; which collects the greatest short-term drop in a preconditioned and
;; filtered time-series of a vegetation index.  Currently, the final
;; function requires three values: (1) time-series as a vector or
;; sequence (2) a scalar parameter indicating the length of the block
;; to which the ordinary least squares regression in applied, which
;; was 15 in the original FORMA specification (3) the length of the
;; window that smooths the OLS coefficients, which was originally 5.

;; TODO: check the values of reliability time-series data to make sure
;; that the good- and bad-set values are correct in
;; make-reliable. This need not be done for the first run, since it
;; wasn't done for the original FORMA application.  All we did was
;; deseasonalize the data, which is reflected below.  It would be a
;; good and feasible (easy) bonus to utilize the reliability data.

(defn short-trend
  "`short-trend` will find the greatest OLS drop over a timeseries `ts`
  given sub-timeseries of length `long-block`.  The drops are smoothed
  by a moving average window of length `window`."
  ([long-block window ts]
     (short-trend ts [] long-block window))
  ([long-block window ts reli-ts]
     (->> (deseasonalize ts)
          (windowed-apply ols-coefficient long-block)
          (windowed-apply coll-avg window)
          (make-monotonic min))))

(defn collect-short-trend
  "preps the short-term drop for the merge into the other data by separating
  out the reference period and the rest of the observations for estimation,
  that is, `for-est`!"
  ([start-pd end-pd long-block window ts]
     (collect-short-trend start-pd end-pd long-block window ts []))
  ([start-pd end-pd long-block window ts reli-ts]
     (let [offset (+ long-block window)
           [y z] (map #(-> % (- offset) (+ 2)) [start-pd end-pd])
           full-ts (short-trend long-block window ts reli-ts)]
       (subvec full-ts (dec y) z))))

;; WHIZBANG :: Collect the long-term drop of a time-series

;; These functions will extract the OLS coefficient associated with a
;; time trend, along with the t-statistic on that coefficient.  For
;; this, we need a time-series of a vegetation index, and a
;; corresponding time-series for rain.  This could be made more
;; general; and in fact, there is a lm (linear model) function in
;; Incanter that can do this work.  This function, however, collects
;; all sorts of other information that we do not need, which may slow
;; processing.  These functions are very tightly tailored to the
;; function that they serve within the total FORMA process.  We assume
;; that we only are every concerned with the coefficient on the time
;; trend (which is reasonable, given the purpose for this routine).  

(defn t-range
  "Provide a range from 1 through the length of a reference vector [v].
  This function was first used to create a time-trend variable to extract
  the linear trend from a time-series (ndvi)."
  [v]
  (map inc (range (count v))))

(defn long-trend-general
  "A general version of the original whizbang function, which extracted the time
  trend and the t-statistic on the time trend from a given time-series.  The more
  general function allows for more attributes to be extracted (e.g., total model
  error).  It also allows for a variable number of cofactors (read: conditioning
  variables) like rainfall.  The time-series is first preconditioned or filtered
  (or both) and then fed into the y-column vector of a linear model;
  the cofactors comprise the cofactor matrix X, which is automatically
  bound to a column of ones for the intercept.  Only the second
  element from each of the attribute vectors is collected; the second
  element is that associated with the time-trend. The try statement is
  meant to check whether the cofactor matrix is singular."
  [attributes t-series & cofactors]
  {:pre [(not (empty? cofactors))]}
  (let [y (deseasonalize (vec t-series))
        X (if (empty? cofactors)
            (i/matrix (t-range y))
            (apply i/bind-columns
                   (t-range y)
                   cofactors))]
    (try
      (map second (map (s/linear-model y X)
                       attributes))
      (catch IllegalArgumentException e
        (repeat (count attributes) 0)))))

(def
  ^{:doc "force the collection of only the OLS coefficients and t-statistics
  from the `long-trend-general` function, since this is what we have used for
  the first implementation of FORMA. `long-trend` takes the same arguments
  as `long-trend-general` except for the first parameter, `attributes`."}
  long-trend
  (partial long-trend-general [:coefs :t-tests]))

(defn lengthening-ts
  "create a sequence of sequences, where each incremental sequence is
  one element longer than the last, pinned to the same starting
  element."
  [start-index end-index base-seq]
  (let [base-vec (vec base-seq)]
    (for [x (range start-index (inc end-index))]
      (subvec base-vec 0 x))))

;; The first vector in `nested-vector` should be the dependent
;; variable, and the rest of the vectors should be the cofactors.
;; `start` and `end` refer to the start and end periods for estimation
;; of long-trend. if `start` == `end` then only the estimation results
;; for the single time period are returned.

(defn collect-long-trend
  "collect the long-term trend coefficient and t-statistic for a
  time-series, after taking into account an arbitrary number of
  cofactors.  `collect-long-trend` returns a map with the trends for
  the reference period separate for the estimation period."
  [start end t-series cofactor-seq]
  (->> (cons t-series cofactor-seq)
       (map (partial lengthening-ts start end))
       (apply map long-trend)))
