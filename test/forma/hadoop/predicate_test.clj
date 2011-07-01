(ns forma.hadoop.predicate-test
  (:use [forma.hadoop.predicate] :reload)
  (:use forma.testing
        cascalog.api
        midje.sweet))

;; Generates combinations of `mod-h`, `mod-v`, `sample` and `line` for
;; use in buffers.

(def pixel-tap
  (let [pix (for [sample (range 10)
                  line   (range 10)
                  :let [val sample]]
              [1 1 sample line val])]
    (<- [?mh ?mv ?s ?l ?v]
        ((vec pix) ?mh ?mv ?s ?l ?v))))

(fact "swap-syms test."
  (swap-syms pixel-tap ["?mh" "?s"] ["?a" "?b"]) => ["?a" "?mv" "?b" "?l" "?v"])

(cascalog.io/with-fs-tmp [_ tmp]
  (tabular
   (fact "Tests of generation off of a series of sequences, be they
   data structure or lazy."
     (fact?- ?seq (lazy-generator tmp ?seq)))
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
 (fact?- ?res (sparse-windower pixel-tap ?dims ?sizes "?v" 0))
 ?dims       ?sizes ?res
 ["?s"]      5      line-set
 ["?s" "?l"] 5      square-set
 ["?s" "?l"] [5 2]  rect-set)
