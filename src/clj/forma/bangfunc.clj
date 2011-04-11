(ns forma.bangfunc
  (:require [incanter.core :as i]))


; test data to simulate a randome time-series (will be read in later)
(def random-ints (repeatedly #(rand-int 100)))
(def test-ts (take 131 random-ints))


; WHOOPBANG

(defn ols-coeff
  "extract OLS coefficient from a time-series
  as efficiently as possible."
  [ts]
  (let [ycol (i/trans [ts])
        pd (count ts)
        X (i/trans (i/bind-rows (repeat pd 1) (range 1 (inc pd))))
        ssX (i/solve (i/mmult (i/trans X) X))]
  (i/sel (i/mmult ssX (i/trans X) ycol) 1 0)))

(def whoopbang (reduce min (map ols-coeff (partition 15 1 test-ts))))
(def whizbang (ols-coeff test-ts))

; juxt function ((juxt a b) x) -> ((a x) (b x))