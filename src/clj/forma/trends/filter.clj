(ns forma.trends.filter
  (:use [forma.utils :only (positions average scale idx)])
  (:require  [forma.matrix.utils :as u]
             [incanter.core :as i]
             [incanter.stats :as s]))

;; Remove seasonal component by basic dummy decomposition

(defn dummy-mat
  "returns an incanter matrix of `n` cycling identity matrices of
  dimension `freq`, used as the cofactor matrix for deseasonalizing a
  time series.

  Example:
    (dummy-mat 23 (count (s/sample-normal 100)))
    ;; 23: frequency of 16-day MODIS data
    ;; 12: frequency of 32-day MODIS data"
  [freq n]
  (i/matrix (take n (cycle (i/identity-matrix freq)))))

(defn deseasonalize
  "accepts a timeseries `ts` with frequency `freq` and returns a
  vector with the seasonal component removed; the returned vector is
  only the trend component and the idiosyncratic disturbance of the
  original time series.

  Precondition avoids singular matrix by throwing exception
  when `ts` is shorter than `freq`. Throws an exception - due to meaningless
  results - if `freq` equals 1 or in case we see `nil` as the `ts`.

  Example:
    (deseasonalize 23 (s/sample-uniform 200))"
  [freq ts]
  {:pre [(>= (count ts) freq)
         (< 1 freq)]}
  (let [x (dummy-mat freq (i/nrow ts))
          xt (i/trans x)
          xtx (i/mmult xt x)
          coefs (i/mmult (i/solve xtx) xt ts)
          fitted (i/mmult x coefs)]
      (i/to-vect (i/plus (i/minus ts fitted)
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
  "returns the matrix of coefficients from the minimization problem
  required to parse the trend component from a time-series of length
  `T`, which has to be greater than or equal to 9 periods."
  [T]
  {:pre  [(>= T 9)]
   :post [(= [T T] (i/dim %))]}
  (let [[first second :as but-2]
        (for [x (range (- T 2))
              :let [idx (if (>= x 2) (- x 2) 0)]]
          (u/insert-into-val 0 idx T (cond (= x 0)  [1 -2 1]
                                           (= x 1)  [-2 5 -4 1]
                                           :else [1 -4 6 -4 1])))]
    (->> [second first]
         (map reverse)
         (concat but-2)
         i/matrix)))

(defn hp-filter
  "return a smoothed time series, given the original time series and
  H-P filter parameter (lambda); from the following reference, we calculate
  inv(lambdaF + I)*y

 Reference: http://goo.gl/VC7jJ"
  [lambda ts]
  (let [T (count ts)
        coeff-matrix (i/mult lambda (hp-mat T))
        trend-cond (i/solve
                    (i/plus coeff-matrix
                            (i/identity-matrix T)))]
    (i/mmult trend-cond ts)))

;; Interpolate unreliable values

(defn interpolate
  "calculate a linear interpolation between `x1` and `x2` with the specified
  `length` between them."
  [x1 x2 length]
  (let [delta (/ (- x2 x1) length)]
    (vec
     (for [n (range length)]
       (float (+ x1 (* n delta)))))))

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
  continuous *bad* values, given by the set of values in `bad-set`
  which serves as a predicate function to (effectively) filter `coll`
  on the ends.  If there are no bad values on either end, then the
  function will return an empty set."
  [bad-set coll]
  (let [m-coll (map-indexed vector coll)
        r-coll (reverse m-coll)]
    (set
     (apply concat
            (map #(for [[m n] % :while (bad-set n)] m)
                 [m-coll r-coll])))))

(defn apply-to-valid
  "apply function `f` to all valid (non-nil) values within a collection `coll`"
  [f coll]
  (f (filter (complement nil?) coll)))

(defn neutralize-ends
  "replace the ends of a value-collection (like NDVI) with the mean of valid values
  if the ends are unreliable, according to an associated reliability index, manifest
  in `reli-coll`. If there are no bad values (as indicated by `bad-set` values in
  `reli-coll`) then `neutralize-ends` will return the original time-series.
  `bad-set` is a set of `reli-coll` values that indicate unreliable pixels."
  [bad-set reli-coll val-coll]
  (let [avg (apply-to-valid u/coll-avg
                            (mask bad-set reli-coll val-coll))]
    (replace-index-set (bad-ends bad-set reli-coll)
                       avg
                       val-coll)))

;; TODODAN: This function works, but it's ugly.  Clean up. And put in
;; pre- and post-conditions.

(defn make-reliable
  "Cleans up a timeseries by replacing or interpolating over low-quality
   or unreliable values, such as those with cloud contamination. `good-set`
   and `bad-set` are Clojure sets used to identify good and bad values.

  This function has two parts: (1) replace bad values at the ends
  with the average of the reliable values in the target coll,
  `value-coll`. (2) smooth over *bad* values, given by `bad-set`,
  which are determined based on the reliability (or quality)
  collection, `quality-coll`.  The `good-set` parameter is a set of
  passable values, presumably interchangeable.  If this assumption is
  not true, then an adjustment will have to be made to this function."
  [good-set bad-set value-coll quality-coll]
  (let [qual-set (set quality-coll)]
    (cond (empty? (seq (clojure.set/intersection good-set qual-set))) nil
          (empty? (clojure.set/difference qual-set good-set)) (vec value-coll)
          :else (let [bad-end-set (bad-ends bad-set quality-coll)
                      new-qual (replace-index-set bad-end-set
                                                  (first good-set)
                                                  quality-coll)
                      new-vals (neutralize-ends bad-set
                                                quality-coll
                                                 value-coll)
                      good-seq (positions (complement bad-set)
                                          new-qual)]
                  (vec (flatten [(map (partial stretch-ts new-vals)
                                      (partition 2 1 good-seq))
                                 (nth new-vals (last good-seq))]))))))

;; Functions to collect cleaning functions for use in the trend
;; feature extraction

(defn tele-ts
  "Create a telescoping sequence of sequences, where each incremental
  sequence is one element longer than the last, pinned to the same
  initial subsequence. `start-index` is interpreted such that the
  first output sequence includes all values up to but not including
  the value at `start-index`. `end-index` is interpreted similarly
  for the final output sequence. `start-index` <= 0 will throw an
  exception to avoid returning empty vector"
  [start-index end-index base-seq]
  {:pre [(> start-index 0)]}
  (let [base-vec (vec base-seq)
        len (count base-vec)]
    (for [x (range start-index (inc end-index))]
      (subvec base-vec 0 (max len x)))))

(defn make-clean
  "Wrapper for `make-reliable`, `deseasonalize` and any future data cleaning
   functions we decide to include. See `make-reliable` and `deseasonalize` for
   further documentation. We use round to remove unwarranted numerical precision
   from cleaned timeseries values."
  [freq good-set bad-set spectral-ts reli-ts]
  (->> (make-reliable good-set bad-set spectral-ts reli-ts)
       (deseasonalize freq)
       (map #(Math/round %))))

(defn reliable?
  "Checks whether the share of reliable pixels exceeds a supplied minimum.

  This should be used to filter out pixels that are too unreliable for analysis, given
   that we use linear interpolation to replace unreliable periods. We have not hardcoded
   a specific threshold, but the papers below provide guidance:

  0.8 reliable in de Beurs (2009) http://dx.doi.org/10.1088/1748-9326/4/4/045012
  0.9 reliable in Verbesselt 2010 http://dx.doi.org/10.1016/j.rse.2010.08.003

  Usage:

  (reliable? #{0 1} 0.9 [0 0 0 1 1 0 1 1 0 0])
  ;=> true

  (reliable? #{0 1} 0.9 [0 0 0 2 2 0 1 1 0 0])
  ;=> false"
  [good-set good-min reli-ts]
  (<= good-min (float
                (/ (count (filter good-set reli-ts))
                   (count reli-ts)))))

(defn shorten-ts
  "Shorten timeseries to length of model timeseries"
  [model-ts ts]
  {:pre [(and (< 0 (count model-ts))
              (< 0 (count ts)))]}
  [(vec (take (count model-ts) ts))])
