(ns forma.source.flame
  (:use cascalog.api
        [forma.date-time :only (convert)])
  (:require [clojure.string :as s]
            [forma.testing :as t]
            [forma.utils :as utils]
            [forma.schema :as schema]
            [forma.reproject :as r]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]))

;; ### Schema Application
;;
;; The fires dataset is different from previous MODIS datasets in that
;; it requires us to keep track of multiple values for each MODIS
;; pixel. We must maintain a total fire count, as well as a count of
;; the subsets of the total that satisfy certain conditions, such as
;; `Temp > 330 Kelvin`, `Confidence > 50`, or both at once. We
;; abstract this complication away by wrapping up each of these into a
;; clojure map. For example:
;;
;;    {:temp-330 1
;;     :conf-50 1
;;     :both-preds 1
;;     :count 2}
;;
;; compound value, represented as a fire-value. We wrap up collections
;; of fire values into a timeseries-value.
;;
;;
;; We'll start with a query that returns, for every day, the date,
;; latitude, longitude and a fire-tuple.

;; need to check whether a fire has a confidence above 330 kelvin. Dan
;; needs count, for every region, number of fires over 330, conf over
;; 50 and the number of both all within a pixel.

;; Function takes kel and limit and returns 1 if kel > limit, 0
;; otherwise.


(def daily-fires-path
  (t/dev-path "/testdata/FireDaily/MCD14DL.2011074.txt"))

(defn to-float [x] (Float. x))

(defn limit-toggle
  [kelvin, limit]
  (if (> (Float. kelvin) limit)
    1 0))

(defn both-preds
  [x y]
  (if (and (= x 1)
           (= y 1))
    1 0))

(defn make-fire-tuples [path]
  (let [src (hfs-textline path)]
    (<- [?lat-float ?lon-float ?date ?fire-value]
        (s/split ?textline #"," :> ?lat ?lon ?kelvin _ _ ?date _ _ ?conf _ _ _)
        (src ?textline)
        (to-float ?lat :> ?lat-float)
        (to-float ?lon :> ?lon-float)
        (limit-toggle ?kelvin 330 :> ?temp-330)
        (limit-toggle ?conf 50 :> ?conf-50)
        (both-preds ?temp-330 ?conf-50 :> ?both-preds)
        (identity 1 :> ?count)
        (schema/fire-value ?temp-330 ?conf-50 ?both-preds ?count :> ?fire-value))))

(def combine-fires (partial merge-with +))

(defparallelagg sum-fires
  :init-var #'identity   
  :combine-var #'combine-fires)

;; [path resolution]
(defn make-test [path, modis-resolution]
  (let [src (make-fire-tuples path)]
    (?<- (stdout)
         ;;[?h ?v ?sample ?line]
         [?lat ?lon ?date ?fire-value]
         (src ?lat ?lon ?date ?fire-value)
         (r/latlon->modis modis-resolution ?lat ?lon :> ?h ?v ?sample ?line)
         (sum-fires ?fire-value :> ?out-tuple)
         )))



;; Next steps -
;; :sort
;; define query that takes make-test query and sort by date, put
;; everything into aggregator that builds timeseries. see
;; (defbufferop). The trick is that we'll be missing timeperiods for
;; fires. Recommend putting data and fire value into the aggregator. 2
;; tuples coming in (period fire-tuple). Convert dates to
;; timeperiods. Hint is reductions.
