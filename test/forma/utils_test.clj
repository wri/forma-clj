(ns forma.utils-test
  (:use forma.utils
        midje.sweet))

(facts "thrush testing!

Here are some examples of how to thread a value through a series of
functions. This has some advantage over threading with `->`, as thrush
is a function, not a macro, and can evaluate its arguments."
  (thrush 2 #(* % 5) (partial + 2)) => 12
  (apply thrush 1 (for [num (range 4)]
                    (fn [x] (+ x num)))) => 7)

(fact "nth-in test."
  (nth-in [[1 2] [3 4]] [0]) => [1 2]
  (nth-in [[1 2] [3 4]] [0 1]) => 2)

(facts "scaling test."
  (scale 2 [1 2 3]) => [2 4 6]
  (scale -1 [1 2 3]) => [-1 -2 -3]
  (scale 0 [2 1]) => [0 0])

(facts "Running sum test."
  (running-sum [] 0 + [1 1 1]) => [1 2 3]
  (running-sum [] 0 + [3 2 1]) => [3 5 6])


;;TODO: update these tests for input-stream.
;;
;; Remove PRECL path.
;;
;; (fact
;;   (with-open [a (input-stream precl-path)]
;;     (type a) => GZIPInputStream))

;; Tests for Byte Manipulation
(fact float-bytes => 4)
