(ns forma.matrix.utils-test
  (:use forma.matrix.utils
        midje.sweet)
  (:require [incanter.core :as i]))

(fact "insert-at tests."
  (insert-at 1 [2 1] [4 5 6]) => [4 2 1 5 6]
  (insert-at 1 [[2 1]] [4 5 6]) => [4 [2 1] 5 6]
  (insert-at 0 [1] [4 5 6]) => [1 4 5 6]
  (insert-at 2 [1 2 3 4] [9 8]) => [9 8 1 2 3 4]
  (insert-at 3 [1 2 3 4] [9 8]) => (throws AssertionError)
  (insert-at -2 [1] [4 5 6]) => (throws AssertionError))

(facts "insert-into-val tests. These first fit nicely."
  (insert-into-val 0 2 5 [1 2 3]) => [0 0 1 2 3]
  (insert-into-val 0 1 4 [1 2 3]) => [0 1 2 3]

  "Can't insert a long seq into a short collection."
  (insert-into-val 0 0 2 [1 2 3]) => (throws AssertionError)

  "Similarly, we can't have a sequence that runs off the end of its
  container."
  (insert-into-val 0 8 10 [1 2 3]) => (throws AssertionError))

(facts "pred-replace."
  (pred-replace #{:cake} 3 [1 2 :cake 4]))

(facts "logical-replace."
  (logical-replace > 3 2 [1 2 5 6]) => [1 2 2 2])

(facts "above-x? testing. Note that `above-x?` returns a partial
function, as per the documentation."
  ((above-x? 5) 5) => falsey
  ((above-x? 4) 5) => truthy)

(facts "coll-avg testing."
  (coll-avg [1 2 3 4]) => 2.5
  (coll-avg []) => (throws AssertionError))

(facts "revolve tests."
  (revolve [1 2 3]) => [[1 2 3] [2 3 1] [3 1 2]]
  (revolve []) => [])

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

(tabular
 (fact "idx and rowcol conversion for squares."
   (idx->rowcol ?edge ?idx) => [?row ?col]
   (rowcol->idx ?edge ?row ?col) => ?idx)
 ?edge ?idx ?row ?col
 10    2    0    2
 10    10   1    0
 100   342  3    42)

(tabular
 (fact "idx and rowcol conversion for rectangles."
   (idx->rowcol ?height ?width ?idx) => [?row ?col]
   (rowcol->idx ?height ?width ?row ?col) => ?idx)
 ?height ?width ?idx ?row ?col
 3       4       2    0    2
 3       4       4    1    0
 9       2       10   5    0
 91     100      342  3    42)

(facts "idx and rowcol conversion, out of bounds."
  "Upper bounds for squares..."
  (idx->rowcol 10 100) => (throws AssertionError)
  (rowcol->idx 10 10 10) => (throws AssertionError)

  "And rectangles."
  (idx->rowcol 3 4 12) => (throws AssertionError)
  (rowcol->idx 3 4 3 2) => (throws AssertionError)

  "Below zero for squares..."
  (idx->rowcol 10 -1) => (throws AssertionError)
  (rowcol->idx 10 -1 0) => (throws AssertionError)

  "And for rectangles."
  (idx->rowcol 10 8 81) => (throws AssertionError)
  (rowcol->idx 10 10 -1 0) => (throws AssertionError))

(tabular
 (fact "Variance Matrix tests. TODODAN: Please expand this with some
 more test cases, including assertion errors."
   (= (variance-matrix ?input) (i/matrix ?output)) => truthy)
 ?input        ?output
 [[1 2] [3 4]] [[5 -3.5] [-3.5 2.5]])

(tabular
 (fact "Column matrix tests."
   (column-matrix ?val ?cols) => ?result)
 ?val ?cols ?result
 2    5     [2 2 2 2 2]
 -1   2     [-1 -1]
 -1   0     []
 [-1] 2     (throws AssertionError)
 2    -1    (throws AssertionError))

(fact "Test of the ones column partial function. Probably
unnecessary."
  (ones-column 4) => [1 1 1 1])

(facts "matrix-of test."
  (matrix-of 2 1 4) => [2 2 2 2]
  (matrix-of 0 2 2) => [[0 0] [0 0]])
