(ns forma.matrix.walk-test
  (:use [forma.matrix.walk] :reload)
  (:use midje.sweet))

(def test-matrix
  [[0 1 2 3 4]
   [5 6 7 8 9]
   [10 11 12 13 14]
   [15 16 17 18 19]
   [20 21 22 23 24]])

(fact
  "test for windowed function"
  (windowed-function max 1 test-matrix) => [6 7 8 9 9
                                            11 12 13 14 14
                                            16 17 18 19 19
                                            21 22 23 24 24
                                            21 22 23 24 24])
