(ns forma.hadoop.predicate-test
  (:use [forma.hadoop.predicate] :reload)
  (:use cascalog.api
        midje.sweet))

;; Generates combinations of `mod-h`, `mod-v`, `sample` and `line` for
;; use in buffers.
(def pixels
  (memory-source-tap
   (for [sample (range 10)
         line   (range 10)
         :let [val sample]]
     [1 1 sample line val])))

(def pixel-tap
  (<- [?mh ?mv ?s ?l ?v]
      (pixels ?mh ?mv ?s ?l ?v)))

(def line-set
  #{[1 1 1 6 [5 6 7 8 9]]
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
    [1 1 0 1 [0 1 2 3 4]]})

(def square-set
  #{[1 1 0 0 [[0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4]]]
    [1 1 0 1 [[0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4] [0 1 2 3 4]]]
    [1 1 1 0 [[5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9]]]
    [1 1 1 1 [[5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9] [5 6 7 8 9]]]})

(def rect-set
  #{[1 1 0 0 [[0 1 2 3 4] [0 1 2 3 4]]]
    [1 1 1 1 [[5 6 7 8 9] [5 6 7 8 9]]]
    [1 1 1 0 [[5 6 7 8 9] [5 6 7 8 9]]]
    [1 1 0 4 [[0 1 2 3 4] [0 1 2 3 4]]]
    [1 1 0 3 [[0 1 2 3 4] [0 1 2 3 4]]]
    [1 1 1 4 [[5 6 7 8 9] [5 6 7 8 9]]]
    [1 1 0 2 [[0 1 2 3 4] [0 1 2 3 4]]]
    [1 1 1 3 [[5 6 7 8 9] [5 6 7 8 9]]]
    [1 1 0 1 [[0 1 2 3 4] [0 1 2 3 4]]]
    [1 1 1 2 [[5 6 7 8 9] [5 6 7 8 9]]]})

(fact "Testing sparse-windower."
  (apply set (??- (sparse-windower pixel-tap ["?s"] 5 "?v" 0))) => line-set
  (apply set (??- (sparse-windower pixel-tap ["?s" "?l"] 5 "?v" 0))) => square-set
  (apply set (??- (sparse-windower pixel-tap ["?s" "?l"] [5 2] "?v" 0))) => rect-set)
