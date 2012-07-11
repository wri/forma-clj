(ns forma.testing-test
  (:use forma.testing :reload)
  (:use [midje sweet])
  (:require [clojure.java.io :as io]))

(facts
  "The development resources path must exist, as well as the testdata
  subdirectory within dev. This test will fail if either directory
  moves."
  (-> (dev-path) io/file .exists) => true?
  (-> (dev-path "/testdata/") io/file .exists) => true?)
