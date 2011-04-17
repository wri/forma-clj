(ns forma.presentation.ndvi-filter
  (:use [forma.matrix.utils :only (replace-val)]
        [forma.date-time :only (msec-range)]
        [forma.trends.filter :only (hp-filter fix-time-series)]
        [clj-time.core :only (date-time)]
        [clojure.contrib.math :only (round expt)])
  (:require [incanter.charts :as c]
            [incanter.core :as i]))

;; This is a presentation to show how the filtering techniques work on
;; time-series data.  We can show the Hodrick-Prescott filter with a
;; slider bar to adjust the smoothing parameter.  We can also show how
;; to interpolate across "holes" as determined by the (fake)
;; reliability time-series

;; Here are some time conversion functions -- note that Sam is being a
;; real bitch about them, so we'll probably be moving these bad boys
;; over to conversion.

(defn round-places
  "Rounds the supplied number to the supplied number of decimal
  points, and returns a float representation."
  [decimals number]
  (let [factor (expt 10 decimals)]
    (float (/ (round (* factor number)) factor))))

;; Create sample NDVI data for a single pixel, so that we can graph
;; the filtering techniques on sample data.

(def ndvi [7417 7568 7930 8049 8039 8533 8260 8192 7968 7148 7724 8800 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538 7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698 7838 8967 8136 7532 7838 8009 8136 8400 8219 8051 8091 7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813 8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216 7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195 8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041 7250 7884 8105 8033 8340 8288 7691 7599 8480 8563 8033 7708 7575 7996 7739 8058 7400 6682 7999 7655 7533 7904 8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878 8524 8167 7526 7330 7325 7485 8108 7978 7035 7650])

;; Create sample reliability time-series, assuming that any NDVI value
;; below 7000 is "bad". A value of "2" indicates an offending value;
;; all other observations are represented by "1".

(def reli (replace-val (replace-val ndvi < 7000 2) > 2 1))

;; Mark all offending values in order to graph the "kill points" 

(def kill-vals (replace-val ndvi > 7000 nil))

;; Create a time range for the x-axis on the time-series graph; NOTE
;; that this will have to change, once the time functions are moved to
;; conversion.

(def forma-range (msec-range (date-time 2000 2) (date-time 2010 12)))

;; Initialize the first plot with the raw NDVI time-series; the
;; default color for this line is blue.  In order to view this plot,
;; you only need to type (i/view plot1) in the REPL.

(def plot1
  (doto (c/time-series-plot forma-range
                            ndvi
                            :title "NDVI with Unreliable Measurements"
                            :x-label ""
                            :y-label ""
                            :legend true
                            :series-label "NDVI")))

;; Add the points indicating bad (read: unreliable) values on the
;; original NDVI time-series.

(c/add-points plot1 forma-range kill-vals :series-label "Unreliable Values")

;; Create a new plot with the HP-filter to be updated by the slider,
;; based on the interpolated NDVI values.

(def plot2 (c/time-series-plot forma-range
                               (hp-filter (fix-time-series #{1} reli ndvi) 2)
                               :title "NDVI and H-P Filter"
                               :x-label ""
                               :y-label ""
                               :legend true
                               :series-label "H-P Filter"))

;; Add a plot of the interpolated NDVI values to serve as the
;; background to the updating slider.

(c/add-lines plot2 forma-range (fix-time-series #{1} reli ndvi) :series-label "Conditioned NDVI")

;; Add the slider to the original time-series plot, defined in the
;; first definition of plot2

(let [x forma-range]
  (c/slider #(i/set-data plot2
                         [x (hp-filter (fix-time-series #{1} reli ndvi) %)])
            (map (partial round-places 2) (range 0 20 0.1))
            "lambda"))

;; Add the original kill plots, just in case we want to show the
;; original scale. Note that we can just turn this feature on and off.

(c/add-points plot2 forma-range kill-vals :shape 1 :series-label "Unreliable Values in Original NDVI Series")



