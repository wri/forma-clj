(ns forma.trends.stretch-test
  (:use forma.trends.stretch
        midje.sweet)
  (:require [forma.date-time :as date]
            [forma.schema :as schema]
            [forma.trends.data :as data]
            [clojure.zip :as zip]))








;; [1 2 3]
;; (1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1)   1/1 - 1/16
;; 1.0
;; (1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2)   1/17 - 2/1
;; 1.0625
;; (2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2)   2/2 - 2/17
;; 2.0


;; (2 2 2 2 2 2 2 2 2 2 2 2 2 3 3 3)   2/18 - 3/5
;; 2.1875

;; should be
;; (2 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3)   2/18 - 3/5



(def ts
  (schema/timeseries-value 384 [1 2 3]))

(fact
  (beg-end "32" "16" (:start-idx ts) (:end-idx ts)) => [736 739])

(fact
  (get-day-seq 384 [1 2 3] 384 1 "32" 0) => [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3])

(fact
  "Starting with January 2002 (non-leap year), period 384, a 3-period \"32\"-day timeseries."
  (ts-expander "32" "16" (schema/timeseries-value
                          384
                          [1 2 3])) => {:start-idx 736
                                        :end-idx 739
                                        :series [1.0 1.0625 2.0 2.3125]})
