(ns forma.test-rain
  (:use [forma.rain] :reload)
  (:use midje.sweet
        clojure.test))

(fact
 (dimensions-at-res 0.5) => [360 720])