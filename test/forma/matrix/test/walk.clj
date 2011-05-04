(ns forma.matrix.walk-test
  (:use forma.matrix.walk)
  (:use midje.sweet))

(def test-mat (for [i (range 5) :let [j (* i 5)]]
                (range j (+ j 5))))

(fact
 "`windowed-function` retains the number of elements of the original matrix"
 (count (windowed-function + 1 test-mat)) => (count (flatten test-mat)))


