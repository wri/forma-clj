(ns forma.notify-test
  (:use [forma.notify] :reload)
  (:use midje.sweet
        [clojure.string :only [split]])
  (:require [clj-time.core :as t]))

(def start-date "2010-01-01")
(def end-date "2011-01-01")
(def sample-dt (t/date-time 2013 3 4 12 1))

(fact
  "Check extract-date-time"
  (extract-date-time sample-dt) => {:date "2013-03-04"
                                    :time "12:01"})

(fact "Test workflow-complete-body."
  (workflow-complete-body start-date end-date sample-dt)
  => "Workflow for 2010-01-01 to 2011-01-01 finished on 2013-03-04 at 12:01.")

(fact "Test step-complete-body."
  (step-complete-body start-date end-date "neighbors" sample-dt)
  => "Step 'neighbors' for 2010-01-01 to 2011-01-01 finished on 2013-03-04 at 12:01.")

(fact "Test pixel-count-body."
  (pixel-count-body start-date end-date 5)
  => "There are 5 pixels in the output for 2010-01-01 to 2011-01-01. There should be 60000000 pixels. This is a difference of 59999995 pixels.")
