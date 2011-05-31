(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use midje.sweet
        clojure.test))

(fact float-bytes => 4)

(fact (floats-for-step 0.5) => 1036800)

