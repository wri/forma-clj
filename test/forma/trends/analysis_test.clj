(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use midje.sweet)
  (:use [forma.presentation.ndvi-filter :only (ndvi reli rain)])
  (:use [forma.trends.filter :only (deseasonalize)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]))

(fact
 "check to make sure the long-trend-general coefficient matches up with
 the straight-up linear model trend coefficient."
 (let [y (deseasonalize (vec ndvi))
       X (i/bind-columns (t-range y) (vec (take 131 rain)))]
   (list (second ((s/linear-model y X) :coefs))))
 => (long-trend-general [:coefs] ndvi rain))

(fact
 "check that short-term trend output is the correct shape for the estimation months"
 (count (:for-est (collect-short-trend 71 75 131 15 5
                                       ndvi (vec reli)))) => (inc (- 131 75)))
