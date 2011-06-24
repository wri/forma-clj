(ns forma.utils-test
  (:use forma.utils
        midje.sweet))

(fact "scaling test."
  (scale 2 [1 2 3]) => [2 4 6]
  (scale -1 [1 2 3]) => [-1 -2 -3]
  (scale 0 [2 1]) => [0 0])

(fact "Running sum test."
  (running-sum [] 0 + [1 1 1]) => [1 2 3]
  (running-sum [] 0 + [3 2 1]) => [3 5 6])
