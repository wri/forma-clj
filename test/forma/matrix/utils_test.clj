(ns forma.matrix.utils-test
  (:use forma.matrix.utils
        midje.sweet))

(tabular
 (fact "Checks on sparse expansion functionality."
   (sparse-expander 0 ?matrix) => ?result)
 ?matrix                             ?result
 [[10 1] [12 4] [15 9] [16 1]]       [1 0 4 0 0 9 1]
 [[10 1] [12 4] [8 1] [15 9] [16 1]] [1 0 4 0 0 9 1]
 [[10 1] [12 4] [8 1] [15 9]]        [1 0 4 0 0 9])

(tabular
 (fact "Sparse expansion can take custom start and length."
   (apply sparse-expander 0 ?matrix ?opts) => ?result)
 ?matrix         ?opts                  ?result
 [[10 1] [12 9]] []                     [1 0 9]
 [[10 1] [12 9]] [:start 8]             [0 0 1 0 9]
 [[10 1] [12 9]] [:start 8 :length 10]  [0 0 1 0 9 0 0 0 0 0]
 [[10 1] [15 9]] [:start 14 :length 3]  [0 9 0])

(facts "matrix-of test."
  (matrix-of 2 1 4) => [2 2 2 2]
  (matrix-of 0 2 2) => [[0 0] [0 0]])
