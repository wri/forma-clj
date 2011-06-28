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

(tabular
 (fact "Test sequify."
   (let [outvars (v/gen-nullable-vars (count (first ?tup-seq)))]
     (sequify (name-vars ?tup-seq outvars))) => ?result)
 ?tup-seq ?result
 [[1 2]]   [[1 2]]
 [[1] [2]] [[1] [2]]
 (<- [?a ?b] ([[1 2]] ?a ?b)) [[1 2]])

(facts "tuples->string testing"
  (??<- [?str] ([1 2 3] ?a) (tuples->string ?a :> ?str)) => [["(1)(2)(3)"]])
