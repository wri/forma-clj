(ns forma.trends.analysis
  (:use [forma.matrix.utils]
        [midje.cascalog]
        [cascalog.api]
        [clojure.math.numeric-tower :only (sqrt floor abs expt)]
        [forma.trends.filter]
        [forma.date-time :as date]
        [clojure.tools.logging :only (error)])
  (:require [forma.utils :as utils]
            [incanter.core :as i]
            [incanter.stats :as s]
            [forma.schema :as schema]))

;; TODO: rearrange functions for better understanding of how things
;; fit together.

;; TODO: replace `s/linear-model` with a custom, pared down function
;; to just grab the t-tests and coefs.
(defn long-stats
  "returns a list with both the value and t-statistic for the OLS
  trend coefficient for a time series, conditioning on a variable
  number of cofactors"
  [ts & cofactors]
  (let [time-step (utils/idx ts)
        X (if (empty? cofactors)
            (i/matrix time-step)
            (apply i/bind-columns time-step cofactors))]
    (try (map second (map (s/linear-model ts X) [:coefs :t-tests]))
         (catch Throwable e
           (error (str "TIMESERIES ISSUES: " ts ", " cofactors) e)))))

(defn linear-residuals
  "returns the residuals from a linear model; cribbed from
  incanter.stats linear model"
  [y x & {:keys [intercept] :or {intercept true}}]
    (let [_x (if intercept (i/bind-columns (replicate (i/nrow x) 1) x) x)
          xtx (i/mmult (i/trans _x) _x)
          coefs (i/mmult (i/solve xtx) (i/trans _x) y)]
      (i/minus y (i/mmult _x coefs))))

(defn first-order-conditions
  "returns a matrix with residual weighted cofactors (incl. constant
  vector) and deviations from mean; corresponds to first-order
  conditions $f_t$ in the following reference: Hansen, B. (1992)
  Testing for Parameter Instability in Linear Models, Journal for
  Policy Modeling, 14(4), pp. 517-533"
  [coll]
  {:pre [(i/matrix? coll)]}
  (let [X (i/matrix (range (count coll)))
        resid (linear-residuals coll X)
        sq-resid (i/mult resid resid)
        mu (utils/average sq-resid)]
    (i/trans (i/bind-columns (i/mult resid X) resid (i/minus sq-resid mu)))))

(defn hansen-stat
  "returns the Hansen (1992) test statistic, based on (1) the first-order
  conditions, and (2) the cumulative first-order conditions."
  [coll]
  (let [foc (first-order-conditions coll)
        focsum (map i/cumulative-sum foc)
        foc-mat (i/mmult foc (i/trans foc))
        focsum-mat (i/mmult focsum (i/trans focsum))]
    (i/trace
     (i/mmult
      (i/solve (i/mult foc-mat (count coll)))
      focsum-mat))))

(defn lengthening-ts
  "create a sequence of sequences, where each incremental sequence is
  one element longer than the last, pinned to the same starting
  element."
  [start-index end-index base-seq]
  (let [base-vec (vec base-seq)]
    (for [x (range start-index (inc end-index))]
      (subvec base-vec 0 x))))

(defn fake-deseasonalize
  [series freq]
  series)

(defn clean-trend
  "Filter out bad values and remove seasonal component for a spectral
  time series (with frequency `freq`) using information in an
  associated reliability time series. Conditions check first whether series
  contains all bad or all good values, and returns nil or the original series,
  respectively"
  [freq spectral-ts reli-ts]
  (let [good-set #{1 0}
        bad-set #{255 3 2}
        reli-set (set reli-ts)]
    (cond (empty? (clojure.set/intersection good-set reli-set)) nil
          (empty? (clojure.set/difference reli-set good-set)) spectral-ts
          :else (-> (make-reliable bad-set good-set reli-ts spectral-ts)
                    (fake-deseasonalize freq)))))

(defn clean-timeseries
  "clean trends (i.e., filter out bad values and remove seasonal
  component) for each intervening time period between `start-idx` and
  `end-idx`.

  TODO: THIS is the most time-intensive function of all the trend
  analysis."
  [freq start-idx end-idx spectral-ts reli-ts]
  (map (partial clean-trend freq)
       (lengthening-ts start-idx end-idx spectral-ts)
       (lengthening-ts start-idx end-idx reli-ts)))

(defn telescoping-long-trend
  "returns a three-tuple with the trend coefficient, trend t-stat, and
  the hansen statistic for each period between `start-idx` (inclusive)
  and `end-idx` (inclusive)."
  [freq start-idx end-idx spectral-ts reli-ts cofactor-ts]
  (let [params [freq start-idx end-idx spectral-ts reli-ts]
        clean-ts (apply clean-timeseries params)
        cofactor-tele (lengthening-ts start-idx end-idx cofactor-ts)]
    (map flatten
         (transpose [(map hansen-stat clean-ts)
                     (map long-stats clean-ts cofactor-tele)]))))

(defn trend-mat
  "returns a (`len` x 2) matrix, with first column of ones; and second
  column of 1 through `len`"
  [len]
  (i/bind-columns
   (repeat len 1)
   (map inc (range len))))

(defn hat-mat
  "returns hat matrix for a trend cofactor matrix of length
  `block-len`, used to calculate the coefficient vector for ordinary
  least squares"
  [block-len]
  (let [X (trend-mat block-len)]
    (i/mmult (i/solve (i/mmult (i/trans X) X))
             (i/trans X))))

(defn grab-trend
  "returns the trend from an ordinary least squares regression of a
  spectral time series on an intercept and a trend variable"
  [proj-mat sub-coll]
  (second (i/mmult proj-mat sub-coll)))

(defn moving-subvec
  "returns a vector of incanter sub-matrices, offset by 1 and of
  length `window`; works like partition for non-incanter data
  structures."
  [window coll]
  (loop [idx 0
         res []]
    (if (> idx (- (count coll) window))
      res
      (recur
       (inc idx)
       (conj res
             (i/sel coll :rows (range idx (+ window idx))))))))

(defn windowed-trend
  "returns a vector of short-term trend coefficients of block length
  `block-len`"
  [block-len freq spectral-ts reli-ts]
  (map (partial grab-trend (hat-mat block-len))
       (moving-subvec block-len
                      (i/matrix (clean-trend freq spectral-ts reli-ts)))))

(defn telescoping-short-trend
  "returns a vector of the short-term trend coefficients over time
  series blocks of length `long-block`, smoothed by moving average
  window of length `short-block`. The coefficients are calculated
  along the entire time-series, but only apply to periods at and after
  the end of the training period."
  [long-block short-block freq training-end end-idx spectral-ts reli-ts]
  (let [leading-buffer (+ 2 (- training-end (+ long-block short-block)))
        results-len (- (inc end-idx) training-end)]
    (->> (windowed-trend long-block freq spectral-ts reli-ts)
         (utils/moving-average short-block)
         (reductions min)
         (drop (dec leading-buffer))
         (take results-len))))
