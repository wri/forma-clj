(ns forma.postprocess.csv
  (:use [cascalog.api]
        [forma.reproject :only (modis->latlon)]
        [forma.date-time :only (period->datetime datetime->period)]
        [clojure.string :only (split join)]
        [forma.utils :only (moving-average average)])
  (:require [cascalog.ops :as c]))

(defn get-val
  [out-map & k]
  (map out-map k))

(defn get-series
  [out-m]
  (vector (:series (:prob-series out-m))))

(defn convert-to-latlon
  [m]
  (vec (apply modis->latlon (map m [:sres :modh :modv :sample :line]))))

(defn csv-ify
  [& args]
  ;; (join "," (apply list* args))
  (apply str (interpose "," (apply flatten [args]))))

(defn float->str-10
  [val]
  (format "%011.10f" val))

(defn biggest
  [[a b]]
  (if (> a b)
    a
    b))

(defn latlon-series-query
  [output-map]
  (<- [?out-str]
      (output-map ?m)
      (get-series ?m :> ?series)
      (convert-to-latlon ?m :> ?lat ?lon)
      (float->str-10 ?lat :> ?str-lat)
      (float->str-10 ?lon :> ?str-lon)
      (csv-ify ?str-lat ?str-lon ?series :> ?out-str)))

(defn mono-inc
  "Make a series of numbers monotonically increasing. Useful for retaining the maximum probability for a pixel over time.

`(mono-inc [3 4 34 34 23 1 34 13 35 35 ])
==> (3 4 34 34 34 34 34 34 35 35)"
  [series]
  (cons (first series) (rest (reductions max series))))

(defn date-no-sep
  [date-str]
  (reduce str (split date-str #"-")))

(defn mk-date-header
  [tres prefix ts]
  (map (partial str prefix)
       (map date-no-sep
            (map
             (partial period->datetime tres)
             (range (:start-idx ts) (inc (:end-idx ts)))))))

(defn not-nil?
  [val]
  (not (nil? val)))

(defn backward-looking-mavg
  "Moving average calculated up to a given element (i.e. looking backwards), rather than starting with a given element. Ensures that only information up to and including given element is incorporated in the moving average.

Expanded timeseries includes pre-pended nils. So moving average is actually forward looking, but starting with non-existing (and filtered out) 'prior' nil values.

Ex. (backward-looking-mavg 3 [1 2 3 4]) expands to [nil nil 1 2 3 4]. Nils are filtered out, so the output is effectively [(average [1]) (average [1 2]) (average [1 2 3]) (average 2 3 4)]"
  [window series]
  (let [expanded-ts (concat (take (dec window) (repeat nil)) series)]
    (map average
         (map #(filter not-nil? %)
              (partition window 1 expanded-ts)))))