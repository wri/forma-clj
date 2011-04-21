(ns forma.trends.analysis
  (:use [forma.matrix.utils :only (variance-matrix average)]
        [forma.trends.filter :only (deseasonalize fix-time-series)]
        [forma.presentation.ndvi-filter :only (ndvi reli rain)]
        [clojure.contrib.math :only (sqrt)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]))

;; NOTE Currently, the NDVI and reliability sample time-series comes from the NDVI
;; trends presentation.  We will have to start a test data file at
;; some point, but I don't want to fuck with the directory structure
;; right now, while I am learning to navigate the project.

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
  "Check to see if the supplied matrix `X` is singular."
  [X]
  (<= (i/det X) 0))

(defn ols-coefficient
  "extract OLS coefficient from a time-series."
  [ts]
  (let [ycol (i/trans [ts])
        X (time-trend-cofactor (count ts))]
    (i/sel (i/mmult (variance-matrix X) (i/trans X) ycol) 1 0)))

(defn windowed-apply 
  "apply a function [f] to a window along a sequence [xs] of length [window]"
  [f window xs]
  (map f (partition window 1 xs)))

;; WHOOPBANG

;; The following is a routine, under the header WHOOPBANG,
;; which collects the greatest short-term drop in a preconditioned and
;; filtered time-series of a vegetation index.  Currently, the final
;; function requires three values: (1) time-series as a vector or
;; sequence (2) a scalar parameter indicating the length of the block
;; to which the ordinary least squares regression in applied, which
;; was 15 in the original FORMA specification (3) the length of the
;; window that smooths the OLS coefficients, which was originally 5.

(defn whoopbang
  "whoopbang will find the greatest OLS drop over a timeseries [ts] given 
  sub-timeseries of length [long-block].  The drops are smoothed by a moving 
  average window of length [window]."
  [ts reli-ts long-block window]
  (->> (fix-time-series ts reli-ts)
       (windowed-apply ols-coefficient long-block)
       (windowed-apply average window)
       (reduce min)))

;; WHIZBANG

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
  (range 1 (inc (count v))))

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
  #_{:pre [(not (empty? cofactors))]}
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

(def long-trend (partial long-trend-general [:coefs :t-tests]))

(defn lengthening-ts
  [base-ts start-index end-index]
  (reverse
   (into ()
         (for [x (range start-index end-index)]
           (take x base-ts)))))

(defn whizbang
  [t-series start-period end-period & cofactors]
  (apply long-trend (lengthening-ts start-period end-period)))
