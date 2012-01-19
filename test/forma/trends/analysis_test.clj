(ns forma.trends.analysis-test
  (:use [forma.trends.analysis] :reload)
  (:use midje.sweet
        [forma.presentation.ndvi-filter :only (ndvi reli rain)])
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
  (idx [1 2 3]) => [0 1 2])

(fact
  (average [1 2 3]) => 2)

(fact
  (moving-average 2 [1 2 3]) => [3/2 5/2])

(tabular
 (fact
   (ols-trend ?v) => ?expected)
 ?v ?expected
 [1 2] 1.
 [1 2 4] 1.5
 [1 2 50] 24.5
 [2 50] 48.)

(fact
   (windowed-map ols-trend 2 [1 2 50]) => [1. 48.])
