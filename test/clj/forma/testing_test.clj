(ns forma.testing-test
  (:use forma.testing :reload)
  (:use cascalog.api
        [midje sweet cascalog])
  (:require [clojure.java.io :as io]))

(fact
  "the development resources path must exist! I don't want to pull
this out of cake or lein, so I've hardcoded it into
`dev-resources-subdir`. I'm counting on this test to fail if the
directory moves."
  (-> (dev-path) io/file .exists) => true?)
