(ns forma.matrix.utils-test
  (:use [forma.matrix.utils] :reload)
  (:use midje.sweet))

(facts "Checks on sparse expansion functionality."
  (sparse-expander 0 [[10 1] [12 4] [15 9] [16 1]]) => [1 0 4 0 0 9 1]
  (sparse-expander 0 [[10 1] [12 4] [8 1] [15 9] [16 1]]) => [1 0 4 0 0 9 1]
  (sparse-expander 0 [[10 1] [12 4] [8 1] [15 9]]) => [1 0 4 0 0 9])

(facts "Sparse expansion can take custom start and length."
  (sparse-expander 0 [[10 1] [12 9]]) => [1 0 9]
  (sparse-expander 0 [[10 1] [12 9]] :start 8) => [0 0 1 0 9]
  (sparse-expander 0 [[10 1] [12 9]] :start 8 :length 10) => [0 0 1 0 9 0 0 0 0 0]
  (sparse-expander 0 [[10 1] [15 9]] :start 14 :length 3) => [0 9 0])
