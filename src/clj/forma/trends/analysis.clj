(ns forma.trends.analysis
  (:use [forma.matrix.utils :only (variance-matrix average)]
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

;; WHOOPBANG

;; The following is a routine, under the header WHOOPBANG,
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

(defn whoop-full
  "`whoop-full` will find the greatest OLS drop over a timeseries `ts`
  given sub-timeseries of length `long-block`.  The drops are smoothed
  by a moving average window of length `window`."
  ([long-block window ts]
     (whoop-full ts [] long-block window))
  ([long-block window ts reli-ts]
     (->> (deseasonalize ts)
          (windowed-apply ols-coefficient long-block)
          (windowed-apply average window)
          (make-monotonic min))))

(defn whoopbang
  "`whoopbang` preps the short-term drop for the merge into the other data
  by separating out the reference period and the rest of the observations
  for estimation, that is, `for-est`!"
  ([ref-pd start-pd end-pd long-block window ts]
     (whoopbang ts [] ref-pd start-pd end-pd long-block window))
  ([ref-pd start-pd end-pd long-block window ts reli-ts]
     (let [offset (+ long-block window)
           [x y z] (map #(-> % (- offset) (+ 2)) [ref-pd start-pd end-pd])
           full-ts (whoop-full long-block window ts reli-ts)]
       {:reference (full-ts (dec x))
        :for-est   (subvec full-ts (dec y) z)})))

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
  (map inc (range (count v))))

(defn test-cof
  [ts & cof]
  (let [y (deseasonalize (vec ts))]
    (do (println (count cof))
        (i/sel 1 (apply i/bind-columns (t-range y) cof)))))

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
  "create a sequence of sequences, where each incremental sequence is one
  element longer than the last, pinned to the same starting element."
  [start-index end-index base-vec]
  (for [x (range start-index (inc end-index))]
    (subvec base-vec 0 x)))

(defn estimate-thread
  "The first vector in `nested-vector` should be the dependent
  variable, and the rest of the vectors should be the cofactors.
  `start` and `end` refer to the start and end periods for estimation
  of long-trend. if `start` == `end` then only the estimation results
  for the single time period are returned."
  [start end nested-vector]
  (->> nested-vector
       (map (partial lengthening-ts start end))
       (apply map long-trend)))

(defn whizbang
  "force whizbang into an output map where the tuple for the reference
  period is separate from the tuples for all other time periods -
  those time periods that are used for est(imation) ... `for-est`!.
  NOTE: the reference, start, and end period for estimation have to
  correspond to the element index from the start of the time-series.
  That is, the date string will have to already be passed through the
  datetime->period function in the date-time namespace.  For example,
  in our original application, the integer 71 (corresponding to Dec 2005)
  is passed in as `ref-pd`."
  [ref-pd start-pd end-pd t-series & cofactors]
  {:pre [(vector? t-series)
         (every? #{true} (vec (map vector? cofactors)))]}
  (let [all-ts (apply vector t-series cofactors)]
    {:reference (flatten (estimate-thread ref-pd ref-pd all-ts))
     :for-est  (estimate-thread start-pd end-pd all-ts)}))

;; BFAST

(def Yt [0.9 0.89 0.88 0.88 0.89 0.9 0.89 0.89 0.87 0.88 0.86 0.83 0.8 0.77 0.76 0.82 0.79 0.82 0.83 0.82 0.84 0.82 0.86 0.89 0.87 0.9 0.88 0.87 0.89 0.9 0.84 0.87 0.85 0.81 0.76 0.74 0.77 0.79 0.75 0.69 0.69 0.69 0.72 0.72 0.76 0.78 0.78 0.82 0.85 0.85 0.83 0.86 0.85 0.86 0.84 0.82 0.82 0.81 0.78 0.75 0.77 0.73 0.75 0.73 0.75 0.77 0.77 0.76 0.77 0.79 0.79 0.79 0.81 0.85 0.84 0.87 0.86 0.82 0.82 0.83 0.81 0.79 0.79 0.76 0.79 0.75 0.77 0.8 0.8 0.83 0.84 0.85 0.85 0.84 0.84 0.84 0.88 0.86 0.85 0.86 0.86 0.86 0.85 0.84 0.73 0.62 0.66 0.58 0.52 0.45 0.42 0.39 0.4 0.42 0.48 0.52 0.56 0.52 0.49 0.49 0.45 0.47 0.53 0.48 0.44 0.42 0.42 0.42 0.38 0.32 0.34 0.31 0.3 0.3 0.3 0.29 0.34 0.32 0.33 0.35 0.36 0.37 0.38 0.38 0.38 0.45 0.4 0.37 0.39 0.38 0.33 0.31 0.33 0.39 0.47 0.43 0.41 0.43 0.43 0.43 0.45 0.46 0.49 0.52 0.55 0.6 0.59 0.65 0.64 0.64 0.61 0.62 0.55 0.54 0.54 0.56 0.53 0.59 0.63 0.68 0.69 0.68 0.74 0.72 0.71 0.68 0.7 0.69 0.73 0.72 0.72 0.73 0.76 0.71 0.76 0.69 0.65 0.64 0.68])

(def Vt [0.885253108230368 0.867280574515708 0.848619763813249 0.848270117939465 0.840321171004965 0.852608740730509 0.83182804577926 0.822557077211613 0.815893287781789 0.845381087138439 0.824984229141628 0.824613794222812 0.828493151321644 0.821154795590607 0.804098703250892 0.868972328861596 0.845399730666543 0.880472193038357 0.882164146510947 0.866473225193443 0.880242490406173 0.840351425904802 0.86456680969001 0.875253108230368 0.847280574515708 0.86861976381325 0.848270117939465 0.820321171004965 0.842608740730509 0.84182804577926 0.772557077211613 0.815893287781789 0.815381087138439 0.774984229141628 0.754613794222812 0.768493151321644 0.821154795590607 0.834098703250892 0.798972328861596 0.745399730666543 0.750472193038357 0.742164146510947 0.766473225193443 0.760242490406173 0.780351425904802 0.78456680969001 0.765253108230368 0.797280574515708 0.818619763813249 0.818270117939465 0.780321171004965 0.812608740730509 0.79182804577926 0.792557077211613 0.785893287781789 0.785381087138439 0.784984229141628 0.804613794222812 0.808493151321644 0.801154795590607 0.814098703250892 0.778972328861596 0.805399730666543 0.790472193038357 0.802164146510947 0.816473225193443 0.810242490406173 0.780351425904802 0.77456680969001 0.775253108230368 0.767280574515708 0.758619763813249 0.778270117939465 0.800321171004965 0.792608740730509 0.81182804577926 0.792557077211613 0.765893287781789 0.785381087138439 0.794984229141628 0.804613794222812 0.818493151321644 0.841154795590607 0.804098703250892 0.838972328861596 0.805399730666543 0.830472193038357 0.852164146510947 0.846473225193443 0.870242490406173 0.860351425904802 0.85456680969001 0.835253108230368 0.817280574515708 0.808619763813249 0.808270117939465 0.830321171004965 0.812608740730509 0.79182804577926 0.792557077211613 0.805893287781789 0.82538108713844 0.814984229141628 0.834613794222812 0.758493151321644 0.671154795590607 0.704098703250892 0.628972328861596 0.575399730666543 0.510472193038357 0.472164146510947 0.436473225193443 0.440242490406173 0.440351425904802 0.48456680969001 0.505253108230368 0.537280574515708 0.488619763813249 0.458270117939465 0.440321171004965 0.402608740730509 0.41182804577926 0.462557077211613 0.425893287781789 0.405381087138439 0.384984229141628 0.414613794222812 0.448493151321644 0.431154795590607 0.364098703250892 0.388972328861596 0.365399730666543 0.360472193038357 0.352164146510947 0.346473225193443 0.330242490406173 0.360351425904803 0.32456680969001 0.315253108230368 0.327280574515708 0.328619763813249 0.338270117939465 0.330321171004965 0.332608740730509 0.32182804577926 0.382557077211613 0.345893287781789 0.335381087138439 0.354984229141628 0.374613794222812 0.358493151321644 0.361154795590607 0.374098703250892 0.438972328861596 0.525399730666543 0.490472193038357 0.462164146510947 0.476473225193443 0.470242490406173 0.450351425904802 0.45456680969001 0.445253108230368 0.467280574515708 0.488619763813249 0.518270117939465 0.550321171004965 0.542608740730509 0.59182804577926 0.572557077211613 0.585893287781789 0.575381087138439 0.584984229141628 0.544613794222812 0.568493151321644 0.591154795590607 0.604098703250892 0.578972328861596 0.645399730666543 0.690472193038357 0.732164146510947 0.736473225193443 0.720242490406173 0.760351425904802 0.72456680969001 0.695253108230368 0.657280574515708 0.668619763813249 0.658270117939465 0.680321171004965 0.672608740730509 0.66182804577926 0.662557077211613 0.705893287781789 0.675381087138439 0.724984229141628 0.684613794222812 0.678493151321644 0.691154795590607 0.724098703250892])

(def ti [2000.13043478261 2000.17391304348 2000.21739130435 2000.26086956522 2000.30434782609 2000.34782608696 2000.39130434783 2000.4347826087 2000.47826086957 2000.52173913043 2000.5652173913 2000.60869565217 2000.65217391304 2000.69565217391 2000.73913043478 2000.78260869565 2000.82608695652 2000.86956521739 2000.91304347826 2000.95652173913 2001 2001.04347826087 2001.08695652174 2001.13043478261 2001.17391304348 2001.21739130435 2001.26086956522 2001.30434782609 2001.34782608696 2001.39130434783 2001.4347826087 2001.47826086957 2001.52173913043 2001.5652173913 2001.60869565217 2001.65217391304 2001.69565217391 2001.73913043478 2001.78260869565 2001.82608695652 2001.86956521739 2001.91304347826 2001.95652173913 2002 2002.04347826087 2002.08695652174 2002.13043478261 2002.17391304348 2002.21739130435 2002.26086956522 2002.30434782609 2002.34782608696 2002.39130434783 2002.4347826087 2002.47826086957 2002.52173913043 2002.5652173913 2002.60869565217 2002.65217391304 2002.69565217391 2002.73913043478 2002.78260869565 2002.82608695652 2002.86956521739 2002.91304347826 2002.95652173913 2003 2003.04347826087 2003.08695652174 2003.13043478261 2003.17391304348 2003.21739130435 2003.26086956522 2003.30434782609 2003.34782608696 2003.39130434783 2003.4347826087 2003.47826086957 2003.52173913043 2003.5652173913 2003.60869565217 2003.65217391304 2003.69565217391 2003.73913043478 2003.78260869565 2003.82608695652 2003.86956521739 2003.91304347826 2003.95652173913 2004 2004.04347826087 2004.08695652174 2004.13043478261 2004.17391304348 2004.21739130435 2004.26086956522 2004.30434782609 2004.34782608696 2004.39130434783 2004.4347826087 2004.47826086957 2004.52173913043 2004.5652173913 2004.60869565217 2004.65217391304 2004.69565217391 2004.73913043478 2004.78260869565 2004.82608695652 2004.86956521739 2004.91304347826 2004.95652173913 2005 2005.04347826087 2005.08695652174 2005.13043478261 2005.17391304348 2005.21739130435 2005.26086956522 2005.30434782609 2005.34782608696 2005.39130434783 2005.4347826087 2005.47826086957 2005.52173913043 2005.5652173913 2005.60869565217 2005.65217391304 2005.69565217391 2005.73913043478 2005.78260869565 2005.82608695652 2005.86956521739 2005.91304347826 2005.95652173913 2006 2006.04347826087 2006.08695652174 2006.13043478261 2006.17391304348 2006.21739130435 2006.26086956522 2006.30434782609 2006.34782608696 2006.39130434783 2006.4347826087 2006.47826086957 2006.52173913043 2006.5652173913 2006.60869565217 2006.65217391304 2006.69565217391 2006.73913043478 2006.78260869565 2006.82608695652 2006.86956521739 2006.91304347826 2006.95652173913 2007 2007.04347826087 2007.08695652174 2007.13043478261 2007.17391304348 2007.21739130435 2007.26086956522 2007.30434782609 2007.34782608696 2007.39130434783 2007.4347826087 2007.47826086957 2007.52173913043 2007.5652173913 2007.60869565217 2007.65217391304 2007.69565217391 2007.73913043478 2007.78260869565 2007.82608695652 2007.86956521739 2007.91304347826 2007.95652173913 2008 2008.04347826087 2008.08695652174 2008.13043478261 2008.17391304348 2008.21739130435 2008.26086956522 2008.30434782609 2008.34782608696 2008.39130434783 2008.4347826087 2008.47826086957 2008.52173913043 2008.5652173913 2008.60869565217 2008.65217391304 2008.69565217391 2008.73913043478])

;; We can!! incorporate rain as a regressor.  If so, this brings the
;; number of regressors to 2, not including the intercept.

;; We will take h as given by the user.  For monthly data, we take h
;; as 12 (greater than number of regressors - 1 for the time trend, 2
;; for the time + rain; less than half the number of observations
;; about 57 for the full archive.) h could also be, like, 10 to allow
;; for multiple breaks within the same year.  I'll do this for now.

(def h 10) 
(def n (count ti))

(defn cumsum
  "compute the running, cumulative sum of a vector"
  [v]
  )

