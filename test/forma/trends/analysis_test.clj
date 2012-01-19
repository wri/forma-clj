(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload
        [clojure.math.numeric-tower :only (sqrt floor abs expt)])
  (:use midje.sweet
        [forma.presentation.ndvi-filter :only (ndvi reli rain Yt)])
  (:require [incanter.core :as i]
            [incanter.stats :as s]))

(fact
  "Check square matrix"
  (let [mat (i/matrix [[1 2 3 4]
                       [5 6 7 8]
                       [9 10 11 12]
                       [13 14 15 16]])]
    (is-square? mat) => true))

(fact
  "check singular matrix"
  (let [mat (i/matrix [[3 6]
                       [1 2]])]
    (singular? singular-matrix) => true))

(fact
  (idx [1 2 3]) => [1 2 3])

(fact
  (first (windowed-map ols-trend 2 [1 2 50])) => (roughly (first [1. 48.])))
(fact
  (last (windowed-map ols-trend 2 [1 2 50])) => (roughly (last [1. 48.])))

(fact
  (transpose [[1 2 3] [4 5 6]] ) => [[1 4] [2 5] [3 6]])

(fact
  (outer-product [1 2 3]) => [1.0 2.0 3.0 2.0 4.0 6.0 3.0 6.0 9.0])

(fact
  (element-sum [[1 2 3] [1 2 3]]) => [2 4 6])

(fact
  (average [1 2 3]) => 2)

(fact
  (moving-average 2 [1 2 3]) => [3/2 5/2])

(tabular
 (fact
   (ols-trend ?v) => ?expected)
 ?v ?expected
 [1 2] (roughly 1. 0.00000001)
 [1 2 4] (roughly 1.5 0.00000001)
 [1 2 50] (roughly 24.5 0.00000001)
 [2 50] (roughly 48. 0.00000001))

(tabular
 "check min-short-trend"
 (fact
   (min-short-trend ?long ?short ?ts) => ?expected)
 ?long ?short ?ts ?expected
 2      1    [1 2 3 4 3 2 1] (roughly -1.)
 3      1    [1 2 3 4 3 2 1] (roughly -1.)
 3      1    [1 2 3 4 3 2 0] (roughly -1.5)
 3      2    [1 2 3 4 3 2 0] (roughly -1.25))

(fact
  "check expt-residuals"
  (let [y Yt
        X (idx Yt)
        power 2]
    (last (expt-residuals y X power))) => 0.04430432657988476)

(tabular
 (fact
   "check scaled-vector"
   (scaled-vector ?scalar ?coll) => ?expected)
 ?scalar ?coll ?expected
 1 [1 2 3] [1 2 3]
 2 [1 2 3] [2 4 6]
 1.5 [1 2 3] [1.5 3.0 4.5]
 )

(defn still-to-do
  "long-stats
   first-order-conditions
   hansen-mats
   trend-stats
   harmonic-series
   k-harmonic-matrix
   harmonic-seasonal-decomposition
  "

  )
