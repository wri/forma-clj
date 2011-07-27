(ns forma.hadoop.predicate-test
  (:use [forma.hadoop.predicate] :reload)
  (:use forma.testing
        cascalog.api
        [midje sweet cascalog]))

;; Generates combinations of `mod-h`, `mod-v`, `sample` and `line` for
;; use in buffers.

(defn pixel-tap [xs]
  (name-vars (vec xs) ["?mh" "?mv" "?s" "?l" "?v"]))

(fact "swap-syms test."
  (swap-syms (pixel-tap [[0 0 0 0 370 0]])
             ["?mh" "?s"]
             ["?a" "?b"]) => ["?a" "?mv" "?b" "?l" "?v"])

(cascalog.io/with-fs-tmp [_ tmp]
  (tabular
   (fact?- "Tests of generation off of a series of sequences, be they
   data structure or lazy."
           ?seq (lazy-generator tmp ?seq))
   ?seq
   [[10] [2] [4] [3]]
   [[1 2] [4 3]]
   (for [x (range 3), y (range 3)] [x y])))

(def line-set
  [[1 1 1 6 [5 6 7 8 9]]
   [1 1 0 0 [0 1 2 3 4]]
   [1 1 1 5 [5 6 7 8 9]]
   [1 1 1 4 [5 6 7 8 9]]
   [1 1 1 3 [5 6 7 8 9]]
   [1 1 1 2 [5 6 7 8 9]]
   [1 1 1 1 [5 6 7 8 9]]
   [1 1 1 0 [5 6 7 8 9]]
   [1 1 0 9 [0 1 2 3 4]]
   [1 1 0 8 [0 1 2 3 4]]
   [1 1 0 7 [0 1 2 3 4]]
   [1 1 0 6 [0 1 2 3 4]]
   [1 1 0 5 [0 1 2 3 4]]
   [1 1 0 4 [0 1 2 3 4]]
   [1 1 1 9 [5 6 7 8 9]]
   [1 1 0 3 [0 1 2 3 4]]
   [1 1 1 8 [5 6 7 8 9]]
   [1 1 0 2 [0 1 2 3 4]]
   [1 1 1 7 [5 6 7 8 9]]
   [1 1 0 1 [0 1 2 3 4]]])

(def square-set
  [[1 1 0 0 [[0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4]]]
   [1 1 0 1 [[0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4]]]
   [1 1 1 0 [[5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9]]]
   [1 1 1 1 [[5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9]]]])

(def rect-set
  [[1 1 0 0 [[0 1 2 3 4] [0 1 2 3 4]]]
   [1 1 1 1 [[5 6 7 8 9] [5 6 7 8 9]]]
   [1 1 1 0 [[5 6 7 8 9] [5 6 7 8 9]]]
   [1 1 0 4 [[0 1 2 3 4] [0 1 2 3 4]]]
   [1 1 0 3 [[0 1 2 3 4] [0 1 2 3 4]]]
   [1 1 1 4 [[5 6 7 8 9] [5 6 7 8 9]]]
   [1 1 0 2 [[0 1 2 3 4] [0 1 2 3 4]]]
   [1 1 1 3 [[5 6 7 8 9] [5 6 7 8 9]]]
   [1 1 0 1 [[0 1 2 3 4] [0 1 2 3 4]]]
   [1 1 1 2 [[5 6 7 8 9] [5 6 7 8 9]]]])

(tabular
 (fact?- "test of sparse-windower capability."
         ?res (-> (pixel-tap (for [sample (range 10)
                                   line   (range 10)
                                   :let [val sample]]
                               [1 1 sample line val]))
                  (sparse-windower ?dims ?sizes "?v" 0)))
 ?dims       ?sizes ?res
 ["?s"]      5      line-set
 ["?s" "?l"] 5      square-set
 ["?s" "?l"] [5 2]  rect-set
 ["?s" "?l"] [5 2]  rect-set)

(fact?- "test of sparse-windower's ability to fill in the blanks."
        [[0 0 0 0 [[10 0 0 0 0]
                   [0 0 0 0 0]]]]
        (sparse-windower (pixel-tap [[0 0 0 0 10]]) ["?s" "?l"] [5 2] "?v" 0))
