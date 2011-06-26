(ns forma.matrix.walk-test
  (:use [forma.matrix.walk] :reload)
  (:use midje.sweet))

(def little-matrix [[0 1 2]
                    [3 4 5]])

(def square-matrix [[:0 :1 :2 :3]
                    [:4 :5 :6 :7]
                    [:8 :9 :a :b]
                    [:c :d :e :f]])

(fact "Walk Matrix tests."
  (walk-matrix square-matrix 3) => [[[:0 :1 :2] [:4 :5 :6] [:8 :9 :a]]
                                    [[:1 :2 :3] [:5 :6 :7] [:9 :a :b]]
                                    [[:4 :5 :6] [:8 :9 :a] [:c :d :e]]
                                    [[:5 :6 :7] [:9 :a :b] [:d :e :f]]])

(fact "buffer matrix testing."
  (buffer-matrix 2 1 little-matrix) => [[1 1 1 1 1 1 1]
                                        [1 1 1 1 1 1 1]
                                        [1 1 0 1 2 1 1]
                                        [1 1 3 4 5 1 1]
                                        [1 1 1 1 1 1 1]
                                        [1 1 1 1 1 1 1]])

(tabular
 (facts "wipe-out test."
   (wipe-out little-matrix ?ks) => ?result)
 ?ks     ?result
 []      little-matrix
 [0]     [[3 4 5]]
 [1]     [[0 1 2]]
 [0 1]   [[0   2]
          [3 4 5]]
 [0 1 2] (throws AssertionError))

(fact "neighbor scan tests."
  (neighbor-scan 1 square-matrix) => [[:0 [[nil nil nil] [nil :1] [nil :4 :5]]]
                                      [:1 [[nil nil nil] [:0 :2] [:4 :5 :6]]]
                                      [:2 [[nil nil nil] [:1 :3] [:5 :6 :7]]]
                                      [:3 [[nil nil nil] [:2 nil] [:6 :7 nil]]]
                                      [:4 [[nil :0 :1] [nil :5] [nil :8 :9]]]
                                      [:5 [[:0 :1 :2] [:4 :6] [:8 :9 :a]]]
                                      [:6 [[:1 :2 :3] [:5 :7] [:9 :a :b]]]
                                      [:7 [[:2 :3 nil] [:6 nil] [:a :b nil]]]
                                      [:8 [[nil :4 :5] [nil :9] [nil :c :d]]]
                                      [:9 [[:4 :5 :6] [:8 :a] [:c :d :e]]]
                                      [:a [[:5 :6 :7] [:9 :b] [:d :e :f]]]
                                      [:b [[:6 :7 nil] [:a nil] [:e :f nil]]]
                                      [:c [[nil :8 :9] [nil :d] [nil nil nil]]]
                                      [:d [[:8 :9 :a] [:c :e] [nil nil nil]]]
                                      [:e [[:9 :a :b] [:d :f] [nil nil nil]]]
                                      [:f [[:a :b nil] [:e nil] [nil nil nil]]]])

(fact "Windowed function testing."
  (let [num-square [[0 1 2 3 4]
                    [5 6 7 8 9]
                    [10 11 12 13 14]
                    [15 16 17 18 19]
                    [20 21 22 23 24]]]
    (windowed-function 1 max num-square) => [6 7 8 9 9
                                             11 12 13 14 14
                                             16 17 18 19 19
                                             21 22 23 24 24
                                             21 22 23 24 24]))
