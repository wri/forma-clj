(ns forma.presentation.ndvi-filter
  (:use [forma.matrix.utils :only (replace-val)]
        [forma.conversion :only (delta-periods)]
        [forma.trends.filter :only (hp-filter fix-time-series)]
        [clj-time.core :only (date-time)]
        [clojure.contrib.math :only (round expt)])
  (:require [clj-time.core :as time]
            [incanter.charts :as c]
            [incanter.core :as i]))

;; This is a presentation to show how the filtering techniques work on
;; time-series data.  We can show the Hodrick-Prescott filter with a
;; slider bar to adjust the smoothing parameter.  We can also show how
;; to interpolate across "holes" as determined by the (fake)
;; reliability time-series

;; Here are some time conversion functions -- note that Sam is being a
;; real bitch about them, so we'll probably be moving these bad boys
;; over to conversion.

(defn round-places [decimals number]
  (let [factor (expt 10 decimals)]
    (float (/ (round (* factor number)) factor))))

(defn msecs-from-epoch
  [date]
  (time/in-msecs (time/interval (time/epoch) date)))

(defn msec-range [start end]
  (let [total-months (inc (delta-periods time/month 1 start end))]
    (for [month (range total-months)]
      (msecs-from-epoch (time/plus start (time/months month))))))

;; Create sample NDVI data for a single pixel, so that we can graph
;; the filtering techniques on sample data. Also a rain time-series.

(def ndvi [7417 7568 7930 8049 8039 8533 8260 8192 7968 7148 7724 8800 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538 7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698 7838 8967 8136 7532 7838 8009 8136 8400 8219 8051 8091 7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813 8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216 7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195 8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041 7250 7884 8105 8033 8340 8288 7691 7599 8480 8563 8033 7708 7575 7996 7739 8058 7400 6682 7999 7655 7533 7904 8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878 8524 8167 7526 7330 7325 7485 8108 7978 7035 7650])

(def rain [0.758979 1.881083 0.450439 0.5629475 1.298105 0.601949 0.633402 2.061727 3.162298 3.909696 0.6729975 0.9873315 3.728811 0.489842 0.1487355 1.223761 1.150318 4.147013 3.660728 1.95627 2.43364 0.561836 2.744055 3.166032 2.693228 0.9457325 4.58443 5.568013 4.3806 4.733245 3.24725 2.76412 5.094728 0.185646 0.1068985 6.126962 1.375724 9.698978 7.819601 7.713306 6.066076 6.647004 3.59003 1.462621 4.674891 2.532174 4.450727 3.375694 4.187068 2.864468 1.170215 10.64091 7.673899 3.436686 1.771399 3.158105 0.5111155 2.208683 2.049198 4.496127 2.553866 7.489977 5.930338 7.039693 8.524044 3.924975 3.303462 3.574769 0.8958245 0.7703965 0.8305405 1.45761 8.588436 4.512204 2.759497 3.36393 2.956733 1.623559 5.497841 5.202463 0.1838125 0.543101 1.729535 2.883465 2.697463 3.44902 9.980256 3.166203 6.295969 3.006878 4.505447 5.265707 0.8314495 0.2807955 1.29249 2.427061 3.392292 6.246957 0.978364 4.386679 5.648489 1.885097 1.558292 6.935847 1.818115 1.517784 3.115736 2.603963 1.950513 6.205854 2.149288 6.715803 6.61316 5.234132 1.221053 1.271832 1.452744 0.146411 1.748276 2.902104 4.295961 10.23304 4.134783 10.14036 13.7435 11.61449 4.125048 4.1153 6.135418 1.551036 0.706522 0.850265 2.239265])


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
            (map (partial round-places 2) (range 0 1000 1))
            "lambda"))

;; Add the original kill plots, just in case we want to show the
;; original scale. Note that we can just turn this feature on and off.

;; (c/add-points plot2 forma-range kill-vals :shape 1 :series-label "Unreliable Values in Original NDVI Series")

;; Initialize the first plot with the raw NDVI time-series; the
;; default color for this line is blue.  In order to view this plot,
;; you only need to type (i/view plot1) in the REPL.

(def plot3
  (doto (c/time-series-plot forma-range
                      ndvi
                      :title ""
                      :x-label ""
                      :y-label ""
                      :legend true
                      :series-label "NDVI")
    (c/set-stroke-color java.awt.Color/green)))


;; Add the points indicating bad (read: unreliable) values on the
;; original NDVI time-series.

(c/add-points plot3 forma-range kill-vals :series-label "UNRELIABLE VALUES")

(doto (c/add-lines plot3 forma-range (hp-filter (fix-time-series #{1} reli ndvi)  2) :series-label "FILTER"))
