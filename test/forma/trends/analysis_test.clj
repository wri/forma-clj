(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use midje.sweet
        [forma.presentation.ndvi-filter :only (ndvi reli rain)]
        [forma.trends.filter :only (deseasonalize)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]))

(fact
  "check to make sure the long-trend-general coefficient matches up
 with the linear model trend coefficient."
  (let [y (deseasonalize (vec ndvi))
        X (i/bind-columns (t-range y) (vec (take 131 rain)))
        {coef :coefs} (s/linear-model y X)]
    (long-trend-general [:coefs] ndvi rain) => [(second coef)]))

(fact
  "check that short-term trend output is the correct shape for the estimation months"
  (let [start 75
        end 131
        final-count (inc (- end start))]
    (count (collect-short-trend start end 15 5 ndvi (vec reli))) => final-count))
