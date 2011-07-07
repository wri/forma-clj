(ns forma.testing-test
  (:use forma.testing
        cascalog.api
        midje.sweet)
  (:require [cascalog.vars :as v]
            [clojure.contrib.io :as io]))

(fact
  "the development resources path must exist! I don't want to pull
this out of cake or lein, so I've hardcoded it into
`dev-resources-subdir`. I'm counting on this test to fail if the
directory moves."
  (-> (dev-path) io/file .exists) => true?)

(facts "tuples->string testing"
  (fact?<- [["(1)(2)(3)"]] [?str] ([1 2 3] ?a) (tuples->string ?a :> ?str)))

;; Tests for cascalog midje stuff.
(defn plis [x y] [[3]])
(defn plas [x y] [[3 5]])

(defn my-query [x y]
  (let [z (plis x y)
        y (plas x y)]
    (<- [?a ?b]
        (z ?a)
        (y ?a ?b))))

(defn a-query [x]
  (<- [?a] (x ?a)))

(fact (plis :a :b) => 10
  (provided (plis :a :b) => 10)
  (against-background (plis :a :b) => 2))

(against-background [(plis :a :b) => 10]
  (fact (plis :a :b) => 10))

(tabular
 (fact?- ?res (apply ?func ?args))
 ?res    ?func    ?args
 [[3 5]] my-query [1 2]
 [[1]]   a-query  [[[1]]])

(def result-seq [[3 5]])
(def some-seq [[10]])

(fact?<- some-seq
         [?a]
         ((plis :a :b) ?a))

(fact?- result-seq (my-query .a. .b.)
        result-seq (my-query .a. .c.)
        (against-background
          (plis .a. .b.) => [[3]]
          (plis .a. .c.) => [[3]]
          (plas .a. .b.) => [[3 5]]
          (plas .a. .c.) => [[3 5]]))

(fact?- [[3 5]] (my-query .a. .b.)
        [[10 15]] (my-query .d. .e.)
        (against-background
          (plis .a. .b.) => [[3]]
          (plas .a. .b.) => [[3 5]]

          (plis .d. .e.) => [[10]]
          (plas .d. .e.) => [[10 15]]))

(fact?<- [[10]]
         [?a]
         ((plis :a :b) ?a)
         (provided (plis :a :b) => [[10]]))
