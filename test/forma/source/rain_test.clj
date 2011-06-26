(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use midje.sweet)
  (:require [forma.testing :as t]))

(def precl-path
  (t/dev-path "/testdata/PRECL/precl_mon_v1.0.lnx.2000.gri0.5m.gz"))

(fact float-bytes => 4)

(fact (floats-for-step 0.5) => 1036800)

