(ns forma.testing-test
  (:use forma.testing
        cascalog.api
        [midje sweet cascalog])
  (:require [cascalog.vars :as v]
            [clojure.contrib.io :as io]))

(fact
  "the development resources path must exist! I don't want to pull
this out of cake or lein, so I've hardcoded it into
`dev-resources-subdir`. I'm counting on this test to fail if the
directory moves."
  (-> (dev-path) io/file .exists) => true?)

(fact?<- "tuples->string testing."
         [["(1)(2)(3)"]] [?str] ([1 2 3] ?a) (tuples->string ?a :> ?str))
