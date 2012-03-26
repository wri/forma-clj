(ns forma.trends.analysis
  (:use [forma.matrix.utils]
        [midje.cascalog]
        [cascalog.api]
        [clojure.math.numeric-tower :only (sqrt floor abs expt)]
        [forma.trends.filter]
        [clojure.tools.logging :only (error)])
  (:require [forma.utils :as utils]
            [incanter.core :as i]
            [incanter.stats :as s]
            [forma.schema :as schema]))

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


(defn linear-residual
  "predict the residual from a coefficient vector"
  [y-val beta-vec x-vec]
  {:pre [(vector? beta-vec) (vector? x-vec)]}
  (- y-val (utils/dot-product beta-vec x-vec)))

(defn inv-mat
  "returns the scale matrix, (X'X)^{-1}, from the normal equations of
  a simple linear regression; note that we pass in the transpose of X,
  since X is large, and we only want to apply transpose once"
  [X Xt]
  (i/solve (i/mmult Xt X)))

(defn ols-beta
  [y-vec X Xt]
  (i/mmult (inv-mat X Xt) Xt y-vec))

(defn recresid-series
  [y X]
  (loop [idx 2
         res [0]
         y-sub (subvec y 0 )]))

;; (defn linear-residuals [y X] (:residuals (s/linear-model y X)))
;;
;; (defn expt-residuals
;;   "returns a list of residuals from the linear regression of `y` on
;;   `X`, raised to `power`"
;;   [power residuals]
;;   (map #(expt % power) residuals))

;; (def X (i/matrix (utils/idx ndvi)))
;; (def y (i/matrix ndvi))
;; (last (expt-residuals y X 2)) => 2640292.6561598806
;; (time (dotimes [_ 100] (dorun (expt-residuals y X 2))))
;; => "Elapsed time: 383.317 msecs"
;; after: (time (dotimes [_ 100] (dorun (expt-residuals y X 2))))
;; "Elapsed time: 320.351 msecs"

;; (defn first-order-conditions
;;   "returns a matrix with residual weighted cofactors (incl. constant
;;   vector) and deviations from mean; corresponds to first-order
;;   conditions $f_t$ in the following reference: Hansen, B. (1992)
;;   Testing for Parameter Instability in Linear Models, Journal for
;;   Policy Modeling, 14(4), pp. 517-533"
;;   [coll]
;;   {:pre [(i/matrix? coll)]}
;;   (let [n (count coll)
;;         X (i/matrix (range n))
;;         resid (linear-residuals coll X)
;;         sq-resid (expt-residuals 2 resid)
;;         mu (utils/average sq-resid)]
;;     (i/bind-rows (map * resid X)
;;                     (map * resid (repeat n 1))
;;                     (map #(- % mu) sq-resid))))



;; (defn foc-mat [coll]
;;   (let [foc (first-order-conditions coll)]
;;     ))

;; (count (last (first-order-conditions ndvi))) => 271
;; (count (first (first-order-conditions ndvi))) => 271
;; (first (last (first-order-conditions ndvi))) => -1352787.304306197
;; (time (dotimes [_ 100] (dorun (first-order-conditions y))))
;; => "Elapsed time: 639.449 msecs"
;; after: (time (dotimes [_ 100] (dorun (first-order-conditions
;; (i/matrix y)))))
;; => "Elapsed time: 248.278 msecs"


;; (defn outer-product2 [coll] (flatten (map #(utils/scale % coll) coll)))

;; (defn hansen-mats
;;   "returns the matrices of element-wise sums of (1) the first-order
;;   conditions, and (2) the cumulative first-order conditions.  This is
;;   only an intermediate step in the calculation of the Hansen (1992)
;;   test for parameter instability in linear models."
;;   [coll]
;;   (let [foc (first-order-conditions coll)
;;         foccum (map i/cumulative-sum foc)]
;;     (vec [
;;           (element-sum (map outer-product (transpose foc)))
;;           (element-sum (map outer-product (transpose foccum)))])))


;; (defn hansen-stat
;;   "returns the Hansen (1992) test statistic; number of first-order
;;   conditions `num-foc` accounts for the demeaned residuals, intercept,
;;   and time-step introduced by `first-order-conditions`"
;;   [coll]
;;   (let [hmat      (hansen-mats coll)
;;         foc       (first hmat) 
;;         cumul-foc (second hmat)]
;;     (print (count cumul-foc))))

    ;; (i/trace
    ;;  (i/mmult
    ;;   (i/solve (i/mult foc (count coll)))
    ;;   cumul-foc))

;; (last (last (hansen-mats (i/matrix y))))
;; 4.0864006033466618E17


;; (hansen-stat (i/matrix ndvi))
;; 0.9113170920764445


;; (defn hansen-stat
;;   "returns the Hansen (1992) test statistic; number of first-order
;;   conditions `num-foc`accounts for the demeaned residuals, intercept,
;;   and time-step introduced by `first-order-conditions`"
;;   [y & x]
;;   (let [num-foc (+ (count x) 3) 
;;         [foc cumul-foc] (apply hansen-mats y x)]
;;     (i/trace
;;      (i/mmult
;;       (i/solve (i/matrix (map #(* (count y) %) foc) num-foc))
;;       (i/matrix cumul-foc num-foc)))))

(defn hansen-stat
  "returns the Hansen (1992) test statistic; number of first-order
  conditions `num-foc`accounts for the demeaned residuals, intercept,
  and time-step introduced by `first-order-conditions`"
  [y]
  1)

(defn lengthening-ts
  "create a sequence of sequences, where each incremental sequence is
  one element longer than the last, pinned to the same starting
  element."
  [start-index end-index base-seq]
  (let [base-vec (vec base-seq)]
    (for [x (range start-index (inc end-index))]
      (subvec base-vec 0 x))))

(defn clean-trend
  "filter out bad values and remove seasonal component for a spectral
  time series (with frequency `freq`) using information in an
  associated reliability time series"
  [freq spectral-ts reli-ts]
  spectral-ts)

(defn clean-tele-trends
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
        clean-ts (apply clean-tele-trends params)
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
