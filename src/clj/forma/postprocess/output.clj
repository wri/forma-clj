(ns forma.postprocess.output
  "Functions to smooth and clean FORMA probability series, and then a
  few supporting functions to assess the accuracy of the estimates by
  counting false negatives, true positives, etc."
  (:use [cascalog.api]
        [forma.reproject :only (modis->latlon)]
        [forma.date-time :only (period->datetime datetime->period)]
        [clojure.string :only (split join)]
        [forma.utils :as u]
        [clojure.math.numeric-tower :only (round)]))

(defn backward-looking-mavg
  "Moving average calculated up to a given element (i.e. looking backwards),
  rather than starting with a given element. Ensures that only
  information up to and including given element is incorporated in the
  moving average.

  Expanded timeseries includes pre-pended nils. So moving average is
  actually forward looking, but starting with non-existing (and
  filtered out) 'prior' nil values.

  For example,

    (backward-looking-mavg 3 [1 2 3 4])

  expands to

    [nil nil 1 2 3 4]

  where the nils are filtered out, so the output is effectively:

    [(average [1]) (average [1 2]) (average [1 2 3]) (average 2 3 4)]

  Example usage:
    (backward-looking-mavg 3 [1 2 3 4]) => '(1.0 1.5 2.0 3.0)"
  [window series]
  (let [expanded-ts (concat (repeat (dec window) nil) (flatten series))]
    (map (comp float average (partial filter #(not (nil? %))))
                  (partition window 1 expanded-ts))))

(defn clean-probs
  "Accepts a timeseries of probabilities, and returns a nested vector
  'cleaned' time series.  Specifically, the cleaned timeseries Smooth
  the probabilities with a backward-looking moving average, make the
  series monotonically increasing, and finally make each probability
  0->1 an integer 0->100.  Note that the moving average window, here,
  is set to 3 periods.  This is hard-coded, since this function's
  expressed purpose is to clean the FORMA probability time series.
  That is, in some sense, FORMA is defined by the 3-length window.

  Example usage:
    (clean-probs [0.1 0.2 0.3 0.4 0.5]) => [[10 15 20 30 40]]"
  [ts nodata]
  {:post [(= (count (flatten %)) (count ts))]}
  (let [no-nodata-ts (u/replace-from-left nodata ts :default 0 :all-types true)]
    [(vec (->> (backward-looking-mavg 3 no-nodata-ts)
             (reductions max)
             (map #(round (* % 100)))))]))

(defn error-map
  "returns a vector indicating one of four possiblities: false
  positive, false negative, true positive, true negative.  This is
  used as an intermediate function to a cascalog query, where the
  order of the elements of the returned vector matters:
  [?false-pos ?true-neg ?true-pos ?false-neg]

  Example use case: 
    (defn error-tap []
      (??<- [?fp-sum ?tn-sum ?tp-sum ?fn-sum]
            (sample-output-map ?m)
            (error-map 0.05 ?m :> ?fp ?tn ?tp ?fn)
            (c/sum ?fp ?tn ?tp ?fn :> ?fp-sum ?tn-sum ?tp-sum ?fn-sum)))"
  [threshold m]
  (let [sig-clearing? (fn [x] (if (> x threshold) true false))
        hansen (:hansen m)
        actual (first (:series (:prob-series m)))]
    (cond (and (zero? hansen) (sig-clearing? actual))                [1 0 0 0]
          (and (zero? hansen) (not (sig-clearing? actual)))          [0 1 0 0]
          (and (not (zero? hansen)) (sig-clearing? actual))          [0 0 1 0]
          (and (not (zero? hansen)) (not (sig-clearing? actual)))    [0 0 0 1])))

(defn error-dict
  [error-tap]
  (let [[error-vec] error-tap]
    (zipmap [:false-pos :true-neg :true-pos :false-neg]
            error-vec)))

(defn precision
  "returns "
  [error-dict]
  (let [true-pos (:true-pos error-dict)]
    (/ true-pos (+ true-pos (:false-pos error-dict)))))

(defn sensitivity
  [error-dict]
  (let [true-pos (:true-pos error-dict)]
    (/ true-pos (+ true-pos (:false-neg error-dict)))))
