(ns forma.trends.analysis
  (:use [forma.matrix.utils :only (variance-matrix coll-avg)]
        [clojure.math.numeric-tower :only (sqrt floor abs expt)]
        [forma.trends.filter :only (deseasonalize make-reliable hp-filter)])
  (:require [forma.utils :as utils]
            [incanter.core :as i]
            [incanter.stats :as s]))

;; TODO (dan): move thest extra matrix functions somewhere else

(defn is-square?
  "returns true if input matrix is square, and false otherwise"
  [mat]
  (let [[row col] (i/dim mat)]
    (= row col)))

(defn singular?
  "returns if square input matrix is singular, and false otherwise"
  [mat]
  {:pre [(is-square? mat)]}
  (<= (i/det mat) 0))

(defn idx
  "return a list of indices starting with 1 equal to the length of
  input"
  [coll]
  (range (count coll)))

(defn average [lst] (/ (reduce + lst) (count lst)))

(defn moving-average [window lst] (map average (partition window 1 lst)))

(defn ols-trend
  "returns the OLS trend coefficient from the input vector; an
  intercept is implicitly assumed"
  [v]
  (let [time-step (idx v)]
    (second (:coefs (s/simple-regression v time-step)))))

(defn windowed-map
  "maps an input function across a sequence of windows onto a vector
  `v` of length `window-len` and offset by 1"
  [f window-len v]
  (pmap f (partition window-len 1 v)))

(defn min-short-trend
  "returns the minimum value of piecewise linear trends of length
  `long-block` on a time series `ts`, after smoothing by moving
  average of window length `short-block`"
  [long-block short-block ts]
  (->> (windowed-map ols-trend long-block ts)
       (moving-average short-block)
       (apply min)))

(defn long-stats
  "returns a list with both the value and t-statistic for the OLS
  trend coefficient for a time series, conditioning on a variable
  number of cofactors"
  [ts & cofactors]
  (let [time-step (idx ts)
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
  (let [time-step (idx y)
        X (if (empty? x)
            (i/matrix time-step)
            (apply i/bind-columns time-step x))
        [resid sq-resid] (map (partial expt-residuals y X) [1 2])]
    (vector (map * resid X)
            (map * resid (repeat (count y) 1))
            (map #(- % (average sq-resid)) sq-resid))))

(defn transpose
  "returns the transposition of a `coll` of vectors"
  [coll]
  (apply (partial map vector) coll))

(defn element-sum
  "returns a vector of sums of each respective element in a vector of vectors,
  i.e., a vector with the first element being the sum of the first
  elements in each sub-vector."
  [coll]
  (apply (partial map +) coll))

(defn hansen-mats
  "returns the matrices of element-wise sums of (1) the first-order
  conditions, and (2) the cumulative first-order conditions.  This is
  only an intermediate step in the calculation of the Hansen (1992)
  test for parameter instability in linear models.
  TODO: remove redundancy, keep readability"
  [y & x]
  (let [foc (apply first-order-conditions y x)]
    [(element-sum (map i/kronecker (transpose foc)))
     (element-sum (map i/kronecker (transpose (map i/cumulative-sum foc))))]))

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

(defn trend-stats
  "return a structured map of the trend characteristics; input the
  spectral time series (e.g., ndvi) and the length (in intervals) of
  the long and short blocks for the short-term drop measurement, along
  with the cofactors used in the long-trend calculations and the break
  magnitude

  NOTE: This is probably not necessary, but it shows what we're trying
  to do."
  [y long-block short-block & x]
  (let [characteristics (create-struct :short :long :long-tstat :break)
        [long-val long-tstat] (apply long-stats y x)]
    (struct characteristics
            (min-short-trend long-block short-block y)
            long-val
            long-tstat
            (apply hansen-stat y x))))

(defn scaled-vector
  [scalar coll]
  (map #(* scalar %) coll))

(defn harmonic-series
  "returns a vector of scaled cosine and sine series of the same
  length as `coll`; the scalar is the harmonic coefficient"
  [freq coll k]
  (let [pds (count coll)
        scalar (/ (* 2 (. Math PI) k) freq)]
    (transpose (map (juxt i/cos i/sin)
                    (scaled-vector scalar (idx coll))))))

(defn k-harmonic-matrix
  "returns an N x (2*k) matrix of harmonic series, where N is the
  length of `coll` and `k` is the number of harmonic terms."
  [freq k coll]
  (let [degree-vector (vec (map inc (range k)))]
    (apply i/bind-columns
           (apply concat
                  (map (partial harmonic-series freq coll)
                       degree-vector)))))

(defn harmonic-seasonal-decomposition
  "returns a deseasonalized time-series; input frequency of
  data (e.g., 23 for 16-day intervals), number of harmonic terms (use
  3 to replicate procedure in the Verbesselt (2010)), and the
  time-series.

  Reference:
  Verbesselt, J. et al. (2010) Phenological Change Detection while
  Accounting for Abrupt and Gradual Trends in Satellite Image Time
  Series, Remote Sensing of Environment, 114(12), 2970–298"
  [freq k coll]
  (let [S (:fitted (s/linear-model coll (k-harmonic-matrix freq k coll)))]
    (map #(+ (average coll) %)
         (apply (partial map -) [coll S]))))



;; ## WHOOPBANG :: Collect the short-term drop associated with a
;; ## time-series

;; The following is a routine, under the header WHOOPBANG (original name),
;; which collects the greatest short-term drop in a preconditioned and
;; filtered time-series of a vegetation index.  Currently, the final
;; function requires three values: (1) time-series as a vector or
;; sequence (2) a scalar parameter indicating the length of the block
;; to which the ordinary least squares regression in applied, which
;; was 15 in the original FORMA specification (3) the length of the
;; window that smooths the OLS coefficients, which was originally 5.

;; TODODAN: check the values of reliability time-series data to make
;; sure that the good- and bad-set values are correct in
;; make-reliable. This need not be done for the first run, since it
;; wasn't done for the original FORMA application.  All we did was
;; deseasonalize the data, which is reflected below.  It would be a
;; good and feasible (easy) bonus to utilize the reliability data.

;; (defn short-trend
;;   "`short-trend` will find the greatest OLS drop over a timeseries `ts`
;;   given sub-timeseries of length `long-block`.  The drops are smoothed
;;   by a moving average window of length `window`."
;;   ([long-block window ts]
;;      (short-trend ts [] long-block window))
;;   ([long-block window ts reli-ts]
;;      (->> (deseasonalize ts)
;;           (windowed-apply ols-coefficient long-block)
;;           (windowed-apply coll-avg window)
;;           (make-monotonic min))))

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
  (take (count v) (iterate inc 1)))

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
  {:pre [(seq cofactors)]}
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

(def long-trend
  "force the collection of only the OLS coefficients and t-statistics
  from the `long-trend-general` function, since this is what we have
  used for the first implementation of FORMA. `long-trend` takes the
  same arguments as `long-trend-general` except for the first
  parameter, `attributes`."
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

;; BFAST-Lite

;; TODO: rename variance-matrix in matrix/utils.clj  This is exactly
;; the same.

(defn xproduct
  " cross-product of a matrix `X`

  Example:
  (xproduct (i/matrix (range 9) 3))
  => [45.0000 54.0000 63.0000
      54.0000 66.0000 78.0000
      63.0000 78.0000 93.0000]
"
  [X]
  (i/mmult (i/trans X) X))

(defn solve-xproduct
  "calculate the inverse - or solve - the cross-product of a matrix
  `X`."
  [X]
  (let [product (xproduct X)]
    (i/solve (if (seq? product)
               product
               (vector product)))))

;; (defn outer-product
;;   " calculate the outer product of two vectors

;;   Example:
;;   (outer-product [1 2 3] [1 2 3])
;;   => [1.0000 2.0000 3.0000
;;       2.0000 4.0000 6.0000
;;       3.0000 6.0000 9.0000]
;; "
;;   [v1 v2]
;;   (i/matrix
;;    (for [e v1] (vec (map (partial * e) v2)))))


(defn lengthening-mat
  "for a matrix of any dimension (including a vector), create a list
  of matrices with rows from the start index through the end of the
  matrix.

  Example:

  (def A (i/matrix [1 2 3 4 5 6 7 8 9] 3))
  (map i/diag (lengthening-mat 0 A))
  => ((1.0) (1.0 5.0) (1.0 5.0 9.0))"
  [start-index coll]
  (let [mat (if (i/matrix? coll) coll (i/matrix coll))]
    (for [i (range start-index (i/nrow mat))]
      (i/sel mat :rows (range (inc i))))))

(defn grab-rows
  " grab a range of rows from a matrix. if two arguments are given, then
  `grab-rows` grabs all rows upto `end-idx`.  if three arguments are
  given the function will grab rows in between the start and ending
  index.  `mat` is the last argument to allow for the function to be
  mapped across many matrices.

  Example:

  (def A (i/matrix (range 21) 3))
  (grab-rows 3 6 A)
  => [ 9.0000 10.0000 11.0000
      12.0000 13.0000 14.0000
      15.0000 16.0000 17.0000]
"
  ([end-idx mat] (i/sel mat :rows (range end-idx)))
  ([start-idx end-idx mat]
     (i/sel mat :rows (range start-idx end-idx))))

(defn regression-coefs
  " create a matrix of regression coefficients from regressing yvec onto
  Xmat; mainly used for linear prediction.

  Example:

  (def y (i/matrix (s/sample-uniform 25)))
  (def X (i/bind-columns (repeat 25 1) (range 25)))
  (regression-coefs y X)
  => [0.4302
      0.0082]
"
  [yvec Xmat]
  (-> (s/simple-regression yvec Xmat :intercept false)
      :coefs
      i/matrix))

(defn linear-predict
  " prediction of y-values from a linear model, for all y values in `yvec`.

  Example:

  (def y (i/matrix (s/sample-uniform 25)))
  (def X (i/bind-columns (repeat 25 1) (range 25)))
  (i/sel (linear-predict y X) :rows (range 4))
  => [0.4302
      0.4384
      0.4466
      0.4548]
"
  [yvec Xmat]
  {:pre [(i/matrix? yvec)]}
  (i/mmult Xmat (regression-coefs yvec Xmat)))

(defn first-index
  " get the first index within `coll` where the value satisfies `pred`.

  Example:
  (first-index #(> % 2) [0 1 2 8 8 8 8])
  => 3"
  [pred coll]
  (first (utils/positions pred coll)))

(defn scaled-sum
  " sum a vector and scale-down by `scalar`

  Example:

  (scaled-sum 2 [1 2 3])
  => 3"
  [scalar coll]
  (-> (reduce + coll)
      (/ scalar)))

(defn num->key
  " turn a number into a keyword for lookup in a map.

  Example:

  (num->key 0.05) => :0.05
  (num->key 0.40) => :0.4"
  [n]
  (keyword (str n)))

(defn recursive-residual
  " calculate the recursive residual at index `idx` from regressing
  yvec on Xmat for all previous observations.  This equation
  corresponds to the equation on page 605 of the cited paper.

  Chu et al. (1995) MOSUM Tests for Parameter Constancy, Biometrika,
  Vol. 82(3).

  Example:

  (def y (i/matrix (s/sample-uniform 25)))
  (def X (i/bind-columns (repeat 25 1) (range 25)))
  (recursive-residual y X 12) => -0.17184202566795484

  ;; note that y will change with each new call, as it is a random
  sample.
"
  [yvec Xmat idx]
  {:pre [(i/matrix? yvec)
         (i/matrix? Xmat)
         (>= idx (inc (i/ncol Xmat)))]}
  (let [[yi Xi] (map #(i/sel % :rows idx) [yvec Xmat])
        [dec-y dec-X] (map (partial grab-rows (dec idx)) [yvec Xmat])
        projection (i/mmult Xi (solve-xproduct dec-X) (i/trans Xi))]
    (-> (i/sel yi 0 0)
        (- (i/mmult Xi (regression-coefs dec-y dec-X)))
        (/ (sqrt (inc projection))))))

(defn recresid-series
  " calcuate recursive residuals for a full set of observations.

  Example *test*:

  (def y (i/matrix (s/sample-uniform 25)))
  (def X (i/bind-columns (repeat 25 1) (range 25)))
  (count (recresid-series y X))
  => 22 ;; (25 - (k + 1)), where k = (i/ncol X)
"
  [yvec Xmat]
  (let [[start end] [(inc (i/ncol Xmat)) (i/nrow Xmat)]]
    (pmap
     (partial recursive-residual yvec Xmat)
     (range start end))))

(defn recresid-sd
  " standard deviation of recursive residual process, found on page 2 of
  the following citation.

  Zeilis, A. (2000) p Values and Alternative Boundaries for CUSUM
  tests. Working Paper 78. AM Working Paper series.

  Example:

  (def y (i/matrix (s/sample-uniform 25)))
  (def X (i/bind-columns (repeat 25 1) (range 25)))
  (recresid-sd y X) => 0.2641384342395973
"
  [yvec Xmat]
  (let [tau (apply - ((juxt i/nrow i/ncol) Xmat))
        residuals (recresid-series yvec Xmat) 
        resid-sum (i/sum-of-squares
                   (pmap
                    #(- % (s/mean residuals))
                    residuals))]
    (sqrt (/ resid-sum tau))))

(defn recresid-map
  "create a map that contains the recursive residual series and
  standard deviation."
  [yvec Xmat]
  (interleave [:series :variance]
              ((juxt recresid-series recresid-sd) yvec Xmat)))

(def crit-value-map
  "Table of critical values from Chu et al. (1995), where the first
  key-level indicates the h value (0.05 - 0.5) which indicates the
  proportion of residuals in each moving sum.  The inner key level
  indicates the significance level.  The float indicates the critical
  value associated with the h-value and significance level, which is
  used to test the *recursive* MOSUM test statistic."
  {:0.05 {:0.2 3.2165 :0.15 3.3185 :0.1 3.4554 :0.05 3.6622 :0.025 3.8632 :0.01 4.1009}
   :0.1  {:0.2 2.9795 :0.15 3.0894 :0.1 3.2368 :0.05 3.4681 :0.025 3.6707 :0.01 3.9397}
   :0.15 {:0.2 2.8289 :0.15 2.9479 :0.1 3.1028 :0.05 3.3382 :0.025 3.5598 :0.01 3.8143}
   :0.2  {:0.2 2.7099 :0.15 2.8303 :0.1 2.9874 :0.05 3.2351 :0.025 3.4604 :0.01 3.7337}
   :0.25 {:0.2 2.6061 :0.15 2.7325 :0.1 2.8985 :0.05 3.1531 :0.025 3.3845 :0.01 3.6626}
   :0.3  {:0.2 2.5111 :0.15 2.6418 :0.1 2.8134 :0.05 3.0728 :0.025 3.3102 :0.01 3.5907}
   :0.35 {:0.2 2.4283 :0.15 2.5609 :0.1 2.7327 :0.05 3.0043 :0.025 3.2461 :0.01 3.5333}
   :0.4  {:0.2 2.3464 :0.15 2.4840 :0.1 2.6605 :0.05 2.9333 :0.025 3.1823 :0.01 3.4895}
   :0.45 {:0.2 2.2686 :0.15 2.4083 :0.1 2.5899 :0.05 2.8743 :0.025 3.1229 :0.01 3.4123}
   :0.5  {:0.2 2.2255 :0.15 2.3668 :0.1 2.5505 :0.05 2.8334 :0.025 3.0737 :0.01 3.3912}})

(defn mosum-efp
  " the recursive, moving-sum Empirical Fluctuation Process (MOSUM
  process), as found in the strucchange package in R, equation 10 in
  the following citation.  Window is equivalent to the `h` parameter
  in strucchange's efp function.  This parameter represents the
  proportion of observations within the moving window - it is
  restricted by the pre-calculated values in `crit-value-map`.

  Zeilis, et al. (2005?) strucchange: An R Package for Testing for
  Structural Change in Linear Regression Models.

  Example (which could be a test):

  (def y (i/matrix (s/sample-uniform 25)))
  (def X (i/bind-columns (repeat 25 1) (range 25)))
  ;; if window = 0.15, then sub-length = 3
  ;; (count (recresid-series y X)) => 22
  (count (mosum-efp y X 0.15)) => 20"
  [yvec Xmat window]
  {:pre [(contains? crit-value-map (num->key window))]}
  (let [tau (apply - ((juxt i/nrow i/ncol) Xmat))
        sub-length (floor (* tau window))
        [series sd] ((juxt recresid-series recresid-sd) yvec Xmat)]
    (pmap (partial scaled-sum (* (sqrt sub-length) sd))
          (partition sub-length 1 series))))

(defn min-mosum-test-statistic
  "collect the absolute value of the most negative value within a
  mosum-efp series."
  [efp-series]
  (abs (reduce min efp-series)))

(defn get-crit-value
  " lookup a critical value based on the window size and significance
  level.

  Example:
  (get-crit-value 0.05 0.05) => 3.6622"
  [window sig-level]
  (-> (num->key window)
      (crit-value-map)
      ((num->key sig-level))))

;; TODO: Make partial-predict more general to split the matrices into
;; more than two pieces

(defn sub-predict
  "predict y-values for sub-sets of the observations: those before and
  those after a break index.  The predictions are based on a linear
  model and are concatenated."
  [yvec Xmat break-idx]
  (let [xx (map i/matrix (split-at break-idx Xmat))
        yy (map i/matrix (split-at break-idx yvec))]
    (mapcat linear-predict yy xx)))

;; TODO: get the break for the largest-break, not just the first
;; significant break.
;;
;; TODO:
;;
;; [row col] ((juxt first second) (i/dim Xmat))
;;
;; is just:
;;
;; [row col] (i/dim Xmat)

(defn mosum-break-idx
  " calculate the period with the first significant downward break in
  a empirical fluctuation process.  If there is no significant break,
  then function will return `nil`.

  Example:

  (mosum-break-idx y X 0.05 0.05) => 111.0
  (mosum-break-idx (range (i/nrow X)) X 0.05 0.05) => nil"
  [yvec Xmat window sig-level]
  (let [neg-crit-value (- (get-crit-value window sig-level))
        [row col] ((juxt first second) (i/dim Xmat))
        offset (-> row (* window) floor (+ col))
        efp-break (first-index #(<= % neg-crit-value)
                               (mosum-efp yvec Xmat window))]
    (when efp-break
      (+ offset efp-break))))

(defn trend-break-magnitude
  " Calculate the magnitude of the breaks associated with a piecewise
  linear regression on a time-series broken at period `break-idx`. The
  equation is given in equation (2) of the following citation.

  Example:

  (break-magnitude Y X 111) => 228.67097

  Vesserbelt et al. (2009) Detecting trend and seasonal changes in
  satellite image time series. Remote Sensing of the Environment;
  found [here](http://goo.gl/y3y1m) on May 31, 2011."
  [yvec Xmat break-idx]
  (let [xx (map i/matrix (split-at break-idx Xmat))
        yy (map i/matrix (split-at break-idx yvec))
        [alpha beta] (apply i/minus
                            (map regression-coefs yy xx))]
    (+ alpha (* beta break-idx))))

(defn mosum-prediction
  " returns a transformed time-series of linear predictions over the
  time-series given by `yvec`, broken at the index of the first
  significant break."
  [yvec Xmat window sig-level]
  (let [break-idx (mosum-break-idx yvec Xmat window sig-level)
        break-map {:break break-idx}]
    (if (nil? break-idx)
      (conj break-map {:series (linear-predict yvec Xmat)})
      (conj break-map {:series (sub-predict yvec Xmat break-idx)}))))

(defn ends-at-split
  "Get a vector of the elements at either end of an index.

  Example:

  (ends-at-split (range 10) 6) => [5 6]

  Note that the index indicates the split just after the indexed
  element.  That is, in the above example, the split occurs after the
  sixth element, which is 5 due to the 0-indexing.  The result vector
  gives the 6th and 7th element."
  [series break-idx]
  (let [coll (if (vector? series) series (vec series))]
    (subvec coll (dec break-idx) (inc break-idx))))

(defn downshift-magnitude
  " Get the magnitude of the first significant drop in the
  MOSUM-analyzed series.

  Example:

  (downshift-magnitude Y X 0.05 0.05) => 0.42154130130818857

  Note that the example is not meant to show an actual value, just
  what the output should look like."
  [yvec Xmat window sig-level]
  (let [predict-map (mosum-prediction yvec Xmat window sig-level)
        [series break] ((juxt :series :break) predict-map)]
    (when break
      (apply - (ends-at-split series break)))))
