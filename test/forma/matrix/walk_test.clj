(ns forma.matrix.walk-test
  (:use [forma.matrix.walk] :reload)
  (:use midje.sweet))

(def little-matrix [[0 1 2]
                    [3 4 5]])

(fact "buffer matrix testing."
  (buffer-matrix 2 1 little-matrix) => [[1 1 1 1 1 1 1]
                                        [1 1 1 1 1 1 1]
                                        [1 1 0 1 2 1 1]
                                        [1 1 3 4 5 1 1]
                                        [1 1 1 1 1 1 1]
                                        [1 1 1 1 1 1 1]])

(def test-matrix [[0 1 2 3 4]
                  [5 6 7 8 9]
                  [10 11 12 13 14]
                  [15 16 17 18 19]
                  [20 21 22 23 24]])

(fact "Windowed function testing."
  (windowed-function 1 max test-matrix) => [6 7 8 9 9
                                            11 12 13 14 14
                                            16 17 18 19 19
                                            21 22 23 24 24
                                            21 22 23 24 24])
