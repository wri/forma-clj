(ns forma.trends.filter
  (:use [forma.utils :only (positions average scale idx)])
  (:require  [forma.matrix.utils :as u]
             [incanter.core :as i]
             [incanter.stats :as s]
             [clojure.set :as set]))

;; Remove seasonal component by basic dummy decomposition

(defn dummy-mat
  "Returns an incanter matrix of `n` cycling identity matrices of
  dimension `freq`, used as the cofactor matrix for deseasonalizing a
  time series.

  Example:
    (use '[forma.trends.data :only (ndvi)])
    (dummy-mat 23 (count ndvi))
    ;; 23: frequency of 16-day MODIS data
    ;; 12: frequency of 32-day MODIS data"
  [freq n]
  {:pre [(>= n freq)]}
  (let [cycle-mat (cycle (i/identity-matrix freq))]
    (i/matrix
     (take n cycle-mat))))

(defn deseasonalize
  "Accepts a timeseries `ts` with cyclical frequency `freq` and
  returns a vector with the seasonal component removed; the returned
  vector is only the trend component and the idiosyncratic disturbance
  of the original time series.

  Precondition avoids singular matrix by throwing exception
  when `ts` is shorter than `freq`. Throws an exception - due to meaningless
  results - if `freq` equals 1 or in case we see `nil` as the `ts`.

  Example:
    (use '[forma.trends.data :only (ndvi)])
    (deseasonalize 23 ndvi)"
  [freq ts]
  {:pre [(>= (i/nrow ts) freq)
         (< 1 freq)]}
  (let [x (dummy-mat freq (i/nrow ts))
        xt (i/trans x)
        xtx (i/mmult xt x)
        coefs (i/mmult (i/solve xtx) xt ts)
        fitted (i/mmult x coefs)]
    (i/to-vect
     (i/plus (i/minus ts fitted)
             (s/mean ts)))))

;; Remove seasonal component by harmonic decomposition

(defn harmonic-series
  "returns a vector of scaled cosine and sine series of the same
  length as `coll`; the scalar is the harmonic coefficient"
  [freq coll k]
  (let [pds (count coll)
        scalar (/ (* 2 (. Math PI) k) freq)]
    (u/transpose (map (juxt i/cos i/sin)
                      (scale scalar (idx coll))))))

(defn k-harmonic-matrix
  "returns an N x (2*k) matrix of harmonic series, where N is the
  length of `coll` and `k` is the number of harmonic terms."
  [freq k coll]
  (let [degree-vector (vec (map inc (range k)))]
    (apply i/bind-columns
           (mapcat (partial harmonic-series freq coll)
                   degree-vector))))

(defn harmonic-seasonal-decomposition
  "returns a deseasonalized time-series; input frequency of
  data (e.g., 23 for 16-day intervals), number of harmonic terms (use
  3 to replicate procedure in the Verbesselt (2010)), and the
  time-series.

  Example:
    (use '[forma.trends.data :only (ndvi)])
    (harmonic-seasonal-decomposition 23 3 ndvi)

  Reference:
    Verbesselt, J. et al. (2010) Phenological Change Detection while
    Accounting for Abrupt and Gradual Trends in Satellite Image Time
    Series, Remote Sensing of Environment, 114(12), 2970-298"
  [freq k coll]
  (let [S (:fitted (s/linear-model coll (k-harmonic-matrix freq k coll)))]
    (map #(-> (- % %2) (+ (average coll)))
         coll S)))

;; Hodrick-Prescott filter for additional smoothing; a higher lambda
;; parameter implies more weight on overall observations, and
;; consequently less weight on recent observations.
;; Reference: http://goo.gl/VC7jJ

(defn hp-mat
  "Returns the matrix of coefficients from the minimization problem
  required to extract the trend component from a time-series of length
  `T`, which has to be greater than or equal to 9 periods."
  [T]
  {:pre  [(>= T 9)]
   :post [(= [T T] (i/dim %))]}
  (let [[first second :as but-2]
        (for [x (range (- T 2))
              :let [idx (if (>= x 2) (- x 2) 0)]]
          (u/insert-into-val 0 idx T (cond (= x 0) [1 -2 1]
                                           (= x 1) [-2 5 -4 1]
                                           :else   [1 -4 6 -4 1])))]
    (->> [second first]
         (map reverse)
         (concat but-2)
         i/matrix)))

(defn hp-filter
  "return a smoothed time series, given the original time series and
  H-P filter parameter (lambda); from the following reference, we calculate
  inv(lambdaF + I)*y; in general, lambda = 1600 is suggested for quarterly
  data.

  Example:
    (use '[forma.trends.data :only (ndvi)])
    (hp-filter 100 ndvi)

  Reference:
    http://goo.gl/hUx2d"
  [lambda ts]
  (let [T (count ts)
        coeff-matrix (i/mult lambda (hp-mat T))
        trend-cond (i/solve
                    (i/plus coeff-matrix
                            (i/identity-matrix T)))]
    (i/mmult trend-cond ts)))

;; Interpolate unreliable values

(defn interpolate
  "calculate a linear interpolation between `x1` and `x2` with the
  specified `length` between them."
  [x1 x2 length]
  {:post [(= (count %) length)]}
  (let [delta (/ (- x2 x1) length)]
    (vec
     (for [n (range length)] (float (+ x1 (* n delta)))))))

(defn stretch-ts
  "stretch time-series across bad values if the left and right values of a
  tuple are not sequential.  The original time-series is `ts` and the tuples
  are a moving window (from `partition 2 1`) based on the valid values from
  an associated time-series of reliability values."
  [ts [left right]]
  (if (= right (inc left))
    (nth ts left)
    (interpolate (nth ts left)
                 (nth ts right)
                 (- right left))))

(defn mask
  "Apply `pred` to `coll-a` to create a mask over `coll-b`, returning a vector of
   valid values and nils. If a value in `coll-a` passes `pred`, the value in the
   corresponding location in `coll-b` will be included in the output vector.
   Otherwise, that location will be set to `nil`."
  [pred coll-a coll-b]
  {:pre [(= (count coll-a) (count coll-b))]}
  (map #(when-not (pred %2) %1)
       coll-b
       coll-a))

(defn replace-index-set
  "replace values in `coll` with `new-val` for all indices in
  `idx-set`"
  [idx-set new-val coll]
  (for [[m n] (map-indexed vector coll)]
    (if (idx-set m) new-val n)))

(defn bad-ends
  "make a set of indices of a collection `coll` for which there are
  continuous bad values, given by the set of values in `bad-set` which
  serves as a predicate function to (effectively) filter `coll` on the
  ends.  If there are no bad values on either end, then the function
  will return an empty set."
  [bad-set coll]
  (let [m-coll (map-indexed vector coll)
        r-coll (reverse m-coll)]
    (set 
     (apply concat
            (map #(for [[m n] % :while (bad-set n)] m)
                 [m-coll r-coll])))))

(defn apply-to-valid
  "apply function `f` to all valid (non-nil) values within a
  collection `coll`"
  [f coll]
  (f (filter (complement nil?) coll)))

(defn neutralize-ends
  "replace the ends of a value-collection (like NDVI) with the mean of
  valid values if the ends are unreliable, according to an associated
  reliability index, manifest in `reli-coll`. If there are no bad
  values (as indicated by `bad-set` values in `reli-coll`) then
  `neutralize-ends` will return the original time-series.  `bad-set`
  is a set of `reli-coll` elements that indicate unreliable pixels."
  [bad-set reli-coll val-coll]
  (let [masked-coll (mask bad-set reli-coll val-coll)
        avg (apply-to-valid u/coll-avg masked-coll)
        bad-idx (bad-ends bad-set reli-coll)]
    (replace-index-set bad-idx avg val-coll)))

(defn restrict-reli
  "returns a sequence of reliability indicators where the bad value
  indicators on the end are replaced with good value indicators; used
  to deal with internal bad values first, and then replace the ends after."
  [good-set bad-set reli-coll]
  (let [bad-end-set (bad-ends bad-set reli-coll)
        representative-good-val (first good-set)]
    (replace-index-set bad-end-set representative-good-val reli-coll)))

(defn good-positions
  "returns the indices of the _internal_ good positions within a
  reliability collection.  Note, however, that the indices of the ends
  of the columns are included in this good-position column, even if
  they are flagged as bad.  The ends are dealt with separately."
  [good-set bad-set reli-coll]
  (let [internal-reli (restrict-reli good-set bad-set reli-coll)]
    (positions (complement bad-set) internal-reli)))

(defn walk-reliable
  "returns a transformed version of `val-coll` with the unreliable (or
  bad) values adjusted.  The adjustment is aimed at reducing the
  amount of noise in the value-column by relying on additional
  information on the reliability of the estimates."
  [good-set bad-set val-coll reli-coll]
  (let [new-vals (neutralize-ends bad-set reli-coll val-coll)
        good-seq (good-positions good-set bad-set reli-coll)
        last-val (nth new-vals (last good-seq))
        interpolated-chunks (map (partial stretch-ts new-vals)
                                 (partition 2 1 good-seq))]
    (flatten [interpolated-chunks last-val])))

(defn make-reliable
  "returns either a nil value, the original value column, or an
  adjusted value column, depending on whether there are any reliable
  values."
  [good-set bad-set val-coll reli-coll]
  (let [reli-set (set reli-coll)]
    (cond (empty? (set/intersection good-set reli-set)) nil
          (empty? (set/difference reli-set good-set)) (vec val-coll)
          :else (vec (walk-reliable good-set bad-set val-coll reli-coll)))))

;; Functions to collect cleaning functions for use in the trend
;; feature extraction

(defn tele-ts
  "create a telescoping sequence of sequences, where each incremental sequence
  is one element longer than the last, pinned to the same initial subsequence.

  `start-index` is interpreted such that the first output sequence
  includes all values up to but not including the value at `start-index`.
  `end-index` is interpreted similarly for the final output sequence.

  `start-index` <= 0 will throw an exception to avoid returning empty vector"
  [start-index end-index base-seq]
  {:pre [(> start-index 0)]}
  (let [base-vec (vec base-seq)]
    (for [x (range start-index (inc end-index))]
      (subvec base-vec 0 x))))

(defn make-clean
  "Wrapper for `make-reliable`, `deseasonalize` and any future data cleaning
   functions we decide to include. See `make-reliable` and `deseasonalize` for
   further documentation. We use round to remove unwarranted numerical precision
   from cleaned timeseries values."
  [freq good-set bad-set spectral-ts reli-ts]
  (let [coll (make-reliable good-set bad-set spectral-ts reli-ts)]
    (if-not (nil? coll) (->> (deseasonalize freq coll) 
                             (map #(Math/round %))))))

(defn reliable?
  "Checks that the share of reliable pixels exceeds a supplied minimum.

  This should be used to filter out pixels that are too unreliable for
  analysis, given that we use linear interpolation to replace
  unreliable periods. We have not hardcoded a specific threshold, but
  the papers below provide guidance:

  0.8 reliable in de Beurs (2009) http://goo.gl/zknDv
  0.9 reliable in Verbesselt (2010) http://goo.gl/daZwN

  Example usage:
    (reliable? #{0 1} 0.9 [0 0 0 1 1 0 1 1 0 0]) => true
    (reliable? #{0 1} 0.9 [0 0 0 2 2 0 1 1 0 0]) => false"
  [good-set good-min reli-ts]
  (<= good-min (float
                (/ (count (filter good-set reli-ts))
                   (count reli-ts)))))

(defn shorten-ts
  "Shorten timeseries to length of model timeseries"
  [model-ts ts]
  [(vec (take (count model-ts) ts))])
