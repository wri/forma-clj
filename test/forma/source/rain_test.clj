(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use midje.sweet))

(fact float-bytes => 4)

(fact (floats-for-step 0.5) => 1036800)

