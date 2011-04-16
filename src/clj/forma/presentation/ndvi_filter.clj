(ns forma.presentation.ndvi-filter
  (:use [forma.matrix.utils :only (replace-val)]
        [forma.conversion :only (delta-periods)]
        [forma.trends.filter :only (hp-filter fix-time-series)]
        [clj-time.core :only (date-time)]
        [clojure.contrib.math :only (round expt)])
  (:require [clj-time.core :as time]
            [incanter.charts :as c]
            [incanter.core :as i]))

;; Here are some time conversion functions -- note that Sam is being a
;; real bitch about them, so we'll probably be moving these bad boys
;; over to conversion.

(defn msecs-from-epoch
  [date]
  (time/in-msecs (time/interval (time/epoch) date)))

(defn msec-range [start end]
  (let [total-months (inc (delta-periods time/month 1 start end))]
    (for [month (range total-months)]
      (msecs-from-epoch (time/plus start (time/months month))))))

(def kill-vals (replace-val ndvi > 7000 nil))
(def reli (replace-val (replace-val ndvi < 7000 2) > 2 1))
(def forma-range (msec-range (date-time 2000 2) (date-time 2010 12)))

(def plot1
  (c/time-series-plot forma-range
                      (hp-filter (fix-time-series #{1} reli ndvi) 2)
                      :title "NDVI and The H-P Filter"
                      :x-label "Year"
                      :y-label "NDVI values"))

;; (c/add-lines plot1 dates ndvi)
#_(c/add-lines plot1
             forma-range
             (fix-time-series #{1} reli ndvi))
;; (c/add-points plot1 dates kill-vals)

(let [x forma-range]
  (c/slider #(i/set-data plot1
                         [x (hp-filter (fix-time-series #{1}
                                                        reli
                                                        ndvi)
                                        (short %))])
            (map (partial round-places 2) (range 0 20 0.1))
            "lambda"))

(defn round-places [decimals number]
  (let [factor (expt 10 decimals)]
    (float (/ (round (* factor number)) factor))))

#_(map (partial round-places 2) (range 0 20 0.1))

(def ndvi [7417 7568 7930 8049 8039 8533 8260 8192 7968 7148 7724 8800 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538 7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698 7838 8967 8136 7532 7838 8009 8136 8400 8219 8051 8091 7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813 8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216 7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195 8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041 7250 7884 8105 8033 8340 8288 7691 7599 8480 8563 8033 7708 7575 7996 7739 8058 7400 6682 7999 7655 7533 7904 8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878 8524 8167 7526 7330 7325 7485 8108 7978 7035 7650])
