(ns forma.trends.analysis
  (:use [forma.matrix.utils :only (variance-matrix average)]
        [forma.trends.filter :only (deseasonalize)]
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
  (i/trans (i/bind-rows (repeat num-months 1) (range 1 (inc num-months)))))
(defn ols-coefficient
  "extract OLS coefficient from a time-series."
  [ts]
  (let [ycol (i/trans [ts])
        X (time-trend-cofactor (count ts))
        V (variance-matrix X)]
    (i/sel (i/mmult V (i/trans X) ycol) 1 0)))

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
  [ts long-block window]
  (->> (deseasonalize ts)
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

(defn with-rain-cofactor
  "construct the cofactors for the Whizbang regression.  This is a
  special case of a more general regression with many more columns.
  This assumes that the time-series are already of the same length."
  [veg-ts rain-ts]
  {:pre (= (count rain-ts) (count veg-ts))}
  (i/bind-columns (time-trend-cofactor (count veg-ts)) (i/matrix rain-ts)))

(defn std-error
  "get the time-trend coefficient's standard error from a variance matrix
  and the vegetation time-series"
  [veg-ts cofactor-matrix var-matrix]
  (let [ycol (i/matrix veg-ts)
        var  (s/variance ycol)]))



;; sum-sq-errors (i/mmult ycol (i/minus (i/identity-matrix (count ycol)) hat-matrix) (i/trans ycol))

(defn ols-coeff
  "get the trend coefficient from a time-series, given a variance matrix"
  [veg-ts cofactor-matrix var-matrix]
  (let [ycol (i/matrix veg-ts)
        betas (i/mmult var-matrix (i/trans cofactor-matrix) ycol)
        error (i/minus ycol (i/mmult cofactor-matrix betas))
        degrees-of-freedom (- (count ycol) (inc (i/ncol betas)))
        select-beta (i/sel betas 0 1)
        ;; std-error (sqrt (/ (* (i/sel var-matrix 0 1) (i/mult (i/trans error) error)) degrees-of-freedom))
        ;; t-stat (/ select-beta std-error)]
    (select-beta))))

;; if [> 0 (i/det var-matrix)]

(defn whizbang
  "extract both the OLS trend coefficient and the t-stat associated
   with the trend characteristic"
  [veg-ts rain-ts]
  (let [cofactor-matrix (with-rain-cofactor veg-ts rain-ts) 
        var-matrix (variance-matrix cofactor-matrix)]
    ((juxt ols-coeff std-error) veg-ts cofactor-matrix var-matrix)))
