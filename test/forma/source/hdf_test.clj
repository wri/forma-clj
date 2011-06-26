(ns forma.source.hdf-test
  (:use [forma.source.hdf] :reload)
  (:use midje.sweet)
  (:require [forma.testing :as t]))

(def hdf-path
  (t/dev-path "/testdata/MOD13A3/MOD13A3.A2000032.h03v11.005.2006271174459.hdf"))
