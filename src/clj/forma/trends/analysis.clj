(ns forma.trends.analysis
  (:use [forma.matrix.utils]
        [clojure.math.numeric-tower :only (sqrt floor abs expt)]
        [forma.date-time :as date]
        [clojure.tools.logging :only (error)])
  (:require [forma.utils :as utils]
            [incanter.core :as i]
            [incanter.stats :as s]))

;; Hansen break statistic

(defn linear-residuals
  "Returns an incanter vector of residuals from a linear model; cribbed from
  incanter.stats linear model.

  Options:
    :intercept (default true)

  Example:
    (use 'forma.trends.data)
    (linear-residuals ndvi (utils/idx ndvi))"
  [y x & {:keys [intercept] :or {intercept true}}]
  (let [_x (if intercept (i/bind-columns (replicate (i/nrow x) 1) x) x)
        xt (i/trans _x)
        xtx (i/mmult xt _x)
        coefs (i/mmult (i/solve xtx) xt y)]
    (i/minus y (i/mmult _x coefs))))

(defn first-order-conditions
  "Returns an incanter matrix of dimension 3xT, where T is the number
  of periods in the supplied time series `ts`.  The values are based
  on a regression of `ts` on a constant and an index. The first row is
  the residual weighted time step variable; the values of the second
  row are the residuals themselves; and the third row is demeaned
  squared residuals.  The matrix collects the first order conditions
  f_t in the Hansen (1992) reference listed below.

  Example:
    (use 'forma.trends.data)
    (first-order-conditions (i/matrix ndvi))

  References:
    Hansen, B. (1992) Testing for Parameter Instability in Linear Models,
    Journal for Policy Modeling, 14(4), pp. 517-533"
  [ts]
  {:pre [(i/matrix? ts)]}
  (let [X (i/matrix (utils/idx ts))
        resid (linear-residuals ts X)
        sq-resid (i/mult resid resid)
        mu (utils/average sq-resid)]
    (i/trans (i/bind-columns
              (i/mult resid X)
              resid
              (i/minus sq-resid mu)))))

(defn hansen-stat
  "Returns the Hansen (1992) test statistic, based on (1) the
  first-order conditions, and (2) the cumulative first-order
  conditions. The try statement will most likely fail if `foc-mat` is
  singular, which will occur when the hansen-stat is applied to a
  constant timeseries.

  Example:
    (hansen-stat ndvi) => 0.9113

  Benchmark:
    (time (dotimes [_ 100] (hansen-stat ndvi)))
    => Elapsed time: 157.507924 msecs"
  [ts]
  (let [ts-mat (i/matrix ts)
        foc (first-order-conditions ts-mat)
        foc-mat (i/mmult foc (i/trans foc))]
    (try
      (let [focsum (map i/cumulative-sum foc)
            focsum-mat (i/mmult focsum (i/trans focsum))]
        (-> (i/solve (i/mult foc-mat (i/nrow ts-mat)))
            (i/mmult focsum-mat)
            (i/trace)))
      (catch Exception e))))

;; Long-term trend characteristic; supporting functions 

(defn trend-characteristics
  "Returns a nested vector of (1) the OLS coefficients from regressing
  a vector `y` on cofactor matrix `x`, and (2) the associated
  t-statistics, ordered appropriately.

  Options:
    :intercept (default true)

  Example:
    (use 'forma.trends.data)
    (trend-characteristics ndvi (utils/idx ndvi))
       => [[7312.6512 -1.1430] [37.4443 -0.9183]]
"
  [y x & {:keys [intercept] :or {intercept true}}]
  (let [_x (if intercept (i/bind-columns (replicate (i/nrow x) 1) x) x)
        xt (i/trans _x)
        xtx (i/mmult xt _x)
        coefs (i/mmult (i/solve xtx) xt y)
        fitted (i/mmult _x coefs)
        resid (i/to-list (i/minus y fitted))
        sse (i/sum-of-squares resid)
        mse (/ sse (- (i/nrow y) (i/ncol _x)))
        coef-var (i/mult mse (i/solve xtx))
        std-errors (i/sqrt (i/diag coef-var))
        t-tests (i/div coefs std-errors)]
    [coefs t-tests]))

(defn long-stats
  "Returns a tuple with the trend coefficient and associated
  t-statistic for the supplied time series `ts`, based on a linear
  regression on an intercept, time step, and a variable number of
  cofactors (e.g., rain).

  Example:
    (use 'forma.trends.data)
    (long-stats ndvi rain) => (-1.2382 -0.9976)
    (long-stats ndvi)      => (-1.1430 -0.9183)"
  [ts & cofactors]
  (let [time-step (utils/idx ts)
        X (if (empty? cofactors)
            (i/matrix time-step)
            (apply i/bind-columns time-step cofactors))]
    (try (map second (trend-characteristics ts X))
         (catch Throwable e
           (error (str "TIMESERIES ISSUES: " ts ", " cofactors) e)))))

;; Short-term trend characteristic; supporting functions

(defn trend-mat
  "Returns a Tx2 incanter matrix, with first column of ones and the
  second column ranging from 1 through T.  Used as a cofactor matrix
  to calculate short-term OLS coefficients.

  Example:
    (trend-mat 10)
    (type (trend-mat 10)) => incanter.Matrix"
  [T]
  (let [ones (repeat T 1)]
    (i/bind-columns ones (utils/idx ones))))

(defn pseudoinverse
  "returns the pseudoinverse of a matrix `x`; the coefficient vector
  of OLS is the dependent variable vector premultiplied by the
  pseudoinverse of the cofactor matrix `x`.  Note that the dimensions
  will be (k x N), where k is the number of regressors and N is the
  number of observations.

  Example:
    (pseudoinverse (trend-mat 10))

  References:
    Moore-Penrose Pseudoinverse (wikipedia): goo.gl/TTJwv"
  [x]
  {:pre [(i/matrix? x)]}
  (let [xt (i/trans x)]
    (i/mmult (i/solve (i/mmult xt x)) xt)))

(defn grab-trend
  "Returns the trend coefficient from an OLS regression of a from an
  ordinary least squares regression of a `coll` of values on an
  intercept and time step.

  Example:
    (use 'forma.trends.data)
    (def pseudo-mat (pseudoinverse (trend-mat 30)))
    (grab-trend pseudo-mat (take 30 ndvi))"
  [pseudo-trend-mat coll]
  (let [v (i/matrix coll)]
    (second (i/mmult pseudo-trend-mat v))))

(defn windowed-trend
  "Returns a vector of short-term trend coefficients of block length
  `block-len`"
  [block-len ts]
  (let [pseudo-mat (pseudoinverse (trend-mat block-len))]
    (map (partial grab-trend pseudo-mat)
         (partition block-len 1 ts))))

(defn short-stat
  "Returns a single value indicating the largest, short-term drop in
  `ts`.  The long-block length indicates the block length over which
  the trend coefficient is calculated; for NDVI analysis, should be of
  length over 1 year.  The short-block length is used to smooth the
  time-series.

  Example:
    (short-stat 30 10 ndvi) => -63.3346"
  [long-block short-block ts]
  (->> (windowed-trend long-block ts)
       (utils/moving-average short-block)
       (reduce min)))
