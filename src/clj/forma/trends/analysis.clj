(ns forma.trends.analysis
  (:use [forma.matrix.utils]
        [clojure.math.numeric-tower :only (sqrt floor abs expt)]
        [forma.trends.filter])
  (:require [forma.utils :as utils]
            [incanter.core :as i]
            [incanter.stats :as s]))

(defn element-sum
  "returns a vector of sums of each respective element in a vector of vectors,
  i.e., a vector with the first element being the sum of the first
  elements in each sub-vector."
  [coll]
  (apply (partial map +) coll))

(defn ols-trend
  "returns the OLS trend coefficient from the input vector; an
  intercept is implicitly assumed"
  [v]
  (let [time-step (utils/idx v)]
    (second (:coefs (s/simple-regression v time-step)))))

(defn min-short-trend
  "returns the minimum value of piecewise linear trends of length
  `long-block` on a time series `ts`, after smoothing by moving
  average of window length `short-block`"
  [long-block short-block ts]
  (->> (utils/windowed-map ols-trend long-block ts)
       (utils/moving-average short-block)
       (apply min)))

(defn long-stats
  "returns a list with both the value and t-statistic for the OLS
  trend coefficient for a time series, conditioning on a variable
  number of cofactors"
  [ts & cofactors]
  (let [time-step (utils/idx ts)
        X (if (empty? cofactors)
            (i/matrix time-step)
            (apply i/bind-columns time-step cofactors))]
    (try
      (map second (map (s/linear-model ts X) [:coefs :t-tests]))
      (catch IllegalArgumentException e))))

(defn expt-residuals
  "returns a list of residuals from the linear regression of `y` on
  `X`, raised to `power`"
  [y X power]
  (map #(expt % power) (:residuals (s/linear-model y X))))

(defn first-order-conditions
  "returns a matrix with residual weighted cofactors (incl. constant
  vector) and deviations from mean; corresponds to first-order
  conditions $f_t$ in the following reference: Hansen, B. (1992)
  Testing for Parameter Instability in Linear Models, Journal for
  Policy Modeling, 14(4), pp. 517-533"
  [y & x]
  (let [time-step (utils/idx y)
        X (if (empty? x)
            (i/matrix time-step)
            (apply i/bind-columns time-step x))
        [resid sq-resid] (map (partial expt-residuals y X) [1 2])]
    (vector (map * resid X)
            (map * resid (repeat (count y) 1))
            (map #(- % (utils/average sq-resid)) sq-resid))))

(defn hansen-mats
  "returns the matrices of element-wise sums of (1) the first-order
  conditions, and (2) the cumulative first-order conditions.  This is
  only an intermediate step in the calculation of the Hansen (1992)
  test for parameter instability in linear models.
  TODO: remove redundancy, keep readability"
  [y & x]
  (let [foc (apply first-order-conditions y x)]
    [(element-sum (map outer-product (transpose foc)))
     (element-sum (map outer-product (transpose (map i/cumulative-sum foc))))]))

(defn hansen-stat
  "returns the Hansen (1992) test statistic; number of first-order
  conditions `num-foc`accounts for the demeaned residuals, intercept,
  and time-step introduced by `first-order-conditions`"
  [y & x]
  (let [num-foc (+ (count x) 3) 
        [foc cumul-foc] (apply hansen-mats y x)]
    (i/trace
     (i/mmult
      (i/solve (i/matrix (map #(* (count y) %) foc) num-foc))
      (i/matrix cumul-foc num-foc)))))

;; TODO: do not duplicate work to "make-reliable" the time series for
;; the various trend analysis functions.

(defn short-trend
  "returns the minumum short-term, smoothed drop from a spectral
  time-series, which as been cleaned (treated for cloud cover) and
  deseasonalized."
  [intervals long-block short-block reli-ts spectral-ts]
  (->> (make-reliable #{3 2} #{1 0} reli-ts spectral-ts)
       (harmonic-seasonal-decomposition intervals 3)
       (min-short-trend long-block short-block)))

(defn long-trend
  [intervals reli-ts spectral-ts]
  (->> (make-reliable #{3 2} #{1 0} reli-ts spectral-ts)
       (harmonic-seasonal-decomposition intervals 3)
       (long-stats)))

(defn lengthening-ts
  "create a sequence of sequences, where each incremental sequence is
  one element longer than the last, pinned to the same starting
  element."
  [start-index end-index base-seq]
  (let [base-vec (vec base-seq)]
    (for [x (range start-index (inc end-index))]
      (subvec base-vec 0 x))))

;; TODO: Find a more efficient way to calculate the running short-term
;; drops, without having to recalculate stat for each, copied time
;; series

;; TODO: Depending on how these functions are called by cascalog,
;; combine to eliminate redundancy

(defn telescoping-short-trend
  "returns a shortened vector of short-term drops for each period
  between `start-pd` and `end-pd`, inclusive."
  [start-pd end-pd intervals long-block short-block spectral-ts reli-ts]
  (let [len-reli (lengthening-ts start-pd end-pd reli-ts)
        len-spectral (lengthening-ts start-pd end-pd spectral-ts)]
    (pmap (partial short-trend intervals long-block short-block)
         len-reli len-spectral)))

;; TODO: check methodology, effect of more observations on statistical
;; significance of down-trend.  That is, check validity of `t-stat`
;; feature in the analysis for projecting forward.  ALL pixels will
;; have higher `t-stats` and higher (in magnitude) short term drops as
;; time goes on.  A sort of one-way ratchet.

(defn telescoping-long-trend
  [start-pd end-pd intervals spectral-ts reli-ts]
    (let [len-reli (lengthening-ts start-pd end-pd reli-ts)
          len-spectral (lengthening-ts start-pd end-pd spectral-ts)]
      (map (partial long-trend intervals)
           len-reli len-spectral)))


(defn deseasonalize
  [freq spectral-ts reli-ts]
  (->> (make-reliable #{3 2} #{1 0} reli-ts spectral-ts)
       (harmonic-seasonal-decomposition 23 3)
       (i/matrix)))

(defn trend-mat
  "returns a (`len` x 2) matrix, with first column of ones; and second
  column of 1 through `len`"
  [len]
  (i/bind-columns
   (repeat len 1)
   (map inc (range len))))

(defn proj-mat
  "returns the projection matrix for a given X, premultiplied by
  inv(X), which is used to calculate the coefficient vector for
  ordinary least squares:

  P = X*inv(X'*X)*X' => inv(X)*P = inv(X'*X)*X',
  which implies that estimated OLS coefficient vector:
  beta = inv(X'*X)*X'*y = inv(X)*P*y"
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
  71[window coll]
  (loop [idx 0
         res []]
    (if (> idx (- (count coll) window))
      res
      (recure
       (inc idx)
       (conj res (i/sel coll
                        :rows (range idx (+ window idx))))))))

(defn windowed-trend
  "returns a vector of short-trem trend coefficients of block length
  `block-len`"
  [block-len freq spectral-ts reli-ts]
  (map (partial grab-trend (proj-mat block-len))
       (moving-subvec block-len (deseasonalize freq spectral-ts reli-ts))))

(defn collect-short-trend
  "returns a vector of the short-term trend coefficients over time
  series blocks of length `long-block`, smoothed by moving average
  window of length `short-block`.  The coefficients are calculated
  along the entire time-series, but only apply to periods at and after
  the end of the training period.  TODO: make sure that the result
  contains the appropriate number of observations, i.e., that the
  first observation in the vector is the interval that marks the end
  of the training period."
  [long-block short-block freq training-end
  spectral-ts reli-ts]
  (let [leading-buffer (+ 2 (- training-end (+ long-block short-block)))]
    (->> (windowed-trend long-block freq spectral-ts reli-ts)
         (utils/moving-average short-block)
         (reductions min)
         (drop leading-buffer))))

