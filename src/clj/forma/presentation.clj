(ns forma.presentation
  "This namespace compiles a set of timeseries graphs to show the
  trend analysis and smoothing functions.  The only function worth
  interacting with is `presentation-view` with a keyword argument.
  For example,

  (presentation-view :hp-filter)

  Other acceptable keywords are :deseas, :harmonic-plot,
  and :break-plot, each showing a plot that illustrates some aspect of
  the timeseries processing."
  (:use [forma.date-time :only (msecs-from-epoch)]
        [forma.trends.data :only (bimonth-ndvi)]
        [forma.trends.filter]
        [forma.trends.bfast]
        [clj-time.core :only (date-time)])
  (:require [incanter.charts :as c]
            [incanter.core :as i]))

(def ndvi-range
  "Bound to a range of times that serve as the x-axis for a timeseries
  plot for the example 16-day NDVI timeseries in forma.trends.data
  namespace.  The elements are time in milliseconds from the initial
  epoch in Jan 1970."
  (take (count bimonth-ndvi)
        (for [yr (range 2000 2010) mo (range 1 13) day [1 15]]
          (msecs-from-epoch (date-time yr mo day)))))

(def deseas-ndvi
  "Bound to the example 16-day NDVI timeseries, deseasonalized by
  dummy decomposition; used throughout the namespace as an example
  series."
  (deseasonalize 23 bimonth-ndvi))

(def decomp-ndvi
  "Bound to the example 16-day NDVI timeseries, deseasonalized by
  harmondic decomposition."
  (harmonic-seasonal-decomposition 23 3 bimonth-ndvi))

(def hp-ndvi
  "Bound to the example 16-day NDVI timeseries, smoothed by HP-filter
  with smoothing parameter set to 10."
  (hp-filter 10 bimonth-ndvi))

(def hp-range
  "Bound to a range of values for the smoothing parameter lambda for the
  HP-filter."
  (let [low-range (map float (range 0 5 0.01))
        high-range (range 5 1600)]
    (concat low-range high-range)))

(defn series-plot
  "Returns an incanter plot, ready for slider adjustment; sets
  parameters for graphing."
  [series]
  (doto (c/time-series-plot ndvi-range series :x-label "" :y-label "")
    (c/set-y-range 0.25 0.92)))

(def plot-efp
  "Define a plot for the empirical fluctuation process, along with
  initial parameters of a relative window length of 0.05 and a p-value
  confidence threshold of 0.05."
  (let [yvec (i/matrix hp-ndvi)
        Xmat (i/bind-columns (repeat (count bimonth-ndvi) 1) ndvi-range)]
    (series-plot (:series (mosum-prediction yvec Xmat 0.05 0.05)))))

(defn add-ts
  "Add a timeseries to the supplied plot of the same length and
  intervals as the example 16-day NDVI series."
  [series plot]
  (c/add-lines plot ndvi-range series :series-label ""))

(defn hp-slider
  "Adds a slider to the specified plot, and the target series defined
  by `x` and `y`."
  [x y plot]
  (c/slider #(i/set-data plot [x (hp-filter % y)]) hp-range "lambda"))

(defn add-bfast-slider
  "Add three sliders to the specified plot showing the break
  statistic, one for the p-value, one for the window length of the
  break stat, and another for the hp-filter parameter."
  [plot]
  (let [x ndvi-range
        ones (repeat (count bimonth-ndvi) 1)]
    (c/sliders*
     #(i/set-data plot [x (:series (mosum-prediction
                                    (i/matrix (hp-filter %1 bimonth-ndvi))
                                    (i/bind-columns ones ndvi-range) %2 %3))])
     [hp-range [0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4 0.45 0.5] [0.01 0.025 0.05 0.1 0.15 0.2]]
     ["h-p filter parameter" "h" "p-value"])))

(defn presentation-view
  "Wrapper function to show the presentation"
  [k]
  (let [hp-plot (series-plot hp-ndvi)
        harmonic-plot (series-plot decomp-ndvi)]
    (cond
     (= k :hp-filter)
     (do (i/view hp-plot)
         (add-ts bimonth-ndvi hp-plot)
         (hp-slider ndvi-range bimonth-ndvi hp-plot))
     (= k :deseas)
     (do (i/view hp-plot)
         (add-ts deseas-ndvi hp-plot)
         (hp-slider ndvi-range deseas-ndvi hp-plot))
     (= k :harmonic-plot)
     (do (i/view harmonic-plot)
         (add-ts bimonth-ndvi  harmonic-plot)
         (hp-slider ndvi-range decomp-ndvi harmonic-plot))
     (= k :break-plot)
     (do (i/view plot-efp)
         (add-ts bimonth-ndvi plot-efp)
         (add-bfast-slider plot-efp)))))
