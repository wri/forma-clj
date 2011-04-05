(ns forma.rain-test
  (:use [forma.rain] :reload)
  (:use midje.sweet clojure.test))

(fact float-bytes => 4)

(facts "" (floats-for-res ))