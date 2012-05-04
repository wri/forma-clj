(ns forma.postprocess.output
  (:use [cascalog.api]
        [forma.reproject :only (modis->latlon)]
        [forma.date-time :only (period->datetime datetime->period)]
        [clojure.string :only (split join)]
        [forma.utils :only (moving-average average)]
        [clojure.math.numeric-tower :only (round)]))

(defn get-val
  [out-m & k]
  (map out-m k))

(defn get-series
  [out-m]
  ((comp vector :series :prob-series) out-m))

(defn convert-to-latlon
  [out-m]
  (vec (apply modis->latlon
              (get-val out-m :sres :modh :modv :sample :line))))

(defn csv-ify
  [& args]
  (apply str (interpose "," (apply flatten [args]))))

(defn float->str-10
  [val]
  (format "%011.10f" val))

(defn not-nil?
  [val]
  (not (nil? val)))

(defn vec-ify
  [series]
  (vector (vec series)))

(defn backward-looking-mavg
  "Moving average calculated up to a given element (i.e. looking backwards), rather than starting with a given element. Ensures that only information up to and including given element is incorporated in the moving average.

Expanded timeseries includes pre-pended nils. So moving average is actually forward looking, but starting with non-existing (and filtered out) 'prior' nil values.

Ex. (backward-looking-mavg 3 [1 2 3 4]) expands to [nil nil 1 2 3 4]. Nils are filtered out, so the output is effectively [(average [1]) (average [1 2]) (average [1 2 3]) (average 2 3 4)]"
  [window series]
  (let [expanded-ts (concat (repeat (dec window) nil) (flatten series))]
    (vec-ify (map (comp float average (partial filter not-nil?))
                  (partition window 1 expanded-ts)))))

(defn mono-inc
  "Make a series of numbers monotonically increasing. Useful for retaining the maximum probability for a pixel over time.

(mono-inc [3 4 34 34 23 1 34 13 35 35 ])
==> (3 4 34 34 34 34 34 34 35 35)"
  [series]
  (vec-ify (cons (first series) (rest (reductions max series)))))

(defn dec->int100
  [coll]
  (map (comp int round (partial * 100)) coll))

(defn clean-probs
  "smooth the probabilities with a backward-looking moving average, make the 
  series monotonically increasing, and finally make each probability 0->1 an 
  integer 0->100"
  [coll]
  (let [window 3]
    [(vec (-> coll
              (backward-looking-mavg window)
              (mono-inc)
              (dec->int100)))]))

(defn date-no-sep
  [date-str]
  (reduce str (split date-str #"-")))

(defn mk-date-header
  [tres prefix ts]
  (map (comp (partial str prefix)
             date-no-sep
             (partial period->datetime tres))
       (range (:start-idx ts) (inc (:end-idx ts)))))

(defn mk-header
  [tres date-prefix ts & args]
  (let [dates (mk-date-header tres date-prefix ts)
        header (csv-ify args)]
    (csv-ify header dates)))
