(ns forma.bangfunc
  (:require [incanter.core :as i]
            [incanter.charts :as c])
  (:use clojure.contrib.def
        clojure.repl
        [clojure.contrib.seq :only (positions)]))

; test data to simulate a randome time-series (will be read in later)
(def random-ints (repeatedly #(rand-int 100)))
(def test-ts (take 131 random-ints))

(def ndvi [7417 7568 7930 8049 8039 8533 8260 8192 7968 7148 7724 8800 8068 7680 7590 7882 8022 8194 8031 8100 7965 8538 7881 8347 8167 5295 8000 7874 8220 8283 8194 7826 8698 7838 8967 8136 7532 7838 8009 8136 8400 8219 8051 8091 7718 8095 8391 7983 8236 8091 7937 7958 8147 8134 7813 8146 7623 8525 8714 8058 6730 8232 7744 8030 8355 8216 7879 8080 8201 7987 8498 7868 7852 7983 8135 8012 8195 8157 7989 8372 8007 8081 7940 7712 7913 8021 8241 8041 7250 7884 8105 8033 8340 8288 7691 7599 8480 8563 8033 7708 7575 7996 7739 8058 7400 6682 7999 7655 7533 7904 8328 8056 7817 7601 7924 7905 7623 7615 7560 7330 7878 8524 8167 7526 7330 7325 7485 8108 7978 7035 7650])

(def pos-test (positions #{#(< % 7000)} ndvi))

(def reli [1])

;; (def rain (vec (take 131 [0.758979 1.881083 0.450439 0.5629475 1.298105 0.601949 0.633402 2.061727 3.162298 3.909696 0.6729975 0.9873315 3.728811 0.489842 0.1487355 1.223761 1.150318 4.147013 3.660728 1.95627 2.43364 0.561836 2.744055 3.166032 2.693228 0.9457325 4.58443 5.568013 4.3806 4.733245 3.24725 2.76412 5.094728 0.185646 0.1068985 6.126962 1.375724 9.698978 7.819601 7.713306 6.066076 6.647004 3.59003 1.462621 4.674891 2.532174 4.450727 3.375694 4.187068 2.864468 1.170215 10.64091 7.673899 3.436686 1.771399 .158105 0.5111155 2.208683 2.049198 4.496127 2.553866 7.489977 5.930338 7.039693 8.524044 3.924975 3.303462 3.574769 0.8958245 0.7703965 0.8305405 1.45761 8.588436 4.512204 2.759497 3.36393 2.956733 1.623559 5.497841 5.202463 0.1838125 0.543101 1.729535 2.883465 2.697463 3.44902 9.980256 3.166203 6.295969 3.006878 4.505447 5.265707 0.8314495 0.2807955 1.29249 2.427061 3.392292 6.246957 0.978364 4.386679 5.648489 1.885097 1.558292 6.935847 1.818115 1.517784 3.115736 2.603963 1.950513 6.205854 2.149288 6.715803 6.61316 5.234132 1.221053 1.271832 1.452744 0.146411 1.748276 2.902104 4.295961 10.23304 4.134783 10.14036 13.7435 11.61449 4.125048 4.1153 6.135418 1.551036 0.706522 0.850265 2.239265]) ))


; test data to simulate a randome time-series (will be read in later)
(def random-ints (repeatedly #(rand-int 100)))
(def test-ts (take 131 random-ints))


; Useful general functions

;; These functions support the following trends analysis. I am not
;; sure where to place them, so that they may be called by other
;; processes. I assume there will be some sort of general utilities file.

(defn average
  "average of a list" 
  [lst] 
  (float (/ (reduce + lst) (count lst))))

(defn variance-matrix
  "construct a variance-covariance matrix from a given matrix X"
  [X]
  (i/solve (i/mmult (i/trans X) X)))

(defn ones-column
  "create a column of ones with length [x] that is compatible with
  other incanter matrices."
  [x]
  (i/matrix (repeat x 1)))

;De-seasonalize time-series

(defn seasonal-rows [n]
  "lazy sequence of monthly dummy vectors"
  (vec
   (take n (partition 11 1 (cycle (cons 1 (repeat 11 0)))))))

(defn seasonal-matrix
  "create a matrix of [num-months]x11 of monthly dummies, where
  [num-months] indicates the number of months in the time-series"
  [num-months]
  (i/bind-columns (ones-column num-months)
                  (seasonal-rows num-months)))

(defn deseasonalize
  "deseasonalize a time-series [ts] using monthly dummies. returns
  a vector the same length of the original time-series, with desea-
  sonalized values."
  [ts]
  (let [avg-seq (repeat (count ts) (average ts))
        X    (seasonal-matrix (count ts))
        fix  (i/mmult (variance-matrix X) (i/trans X) ts)
        adj  (i/mmult (i/sel X :except-cols 0) (i/sel fix :except-rows 0))]
    (i/minus ts adj)))


; WHOOPBANG
 
(defn whoop-cofactor
  "construct a matrix of cofactors; first column is comprised of ones,
  second column is a range from 1 to [num-months].  The result is a
  [num-months]x2 incanter matrix."
  [num-months]
  (i/trans (i/bind-rows (repeat num-months 1) (range 1 (inc num-months)))))

(defn whoop-ols
  "extract OLS coefficient from a time-series as efficiently 
  as possible for the whoopbang function."
  [ts]
  (let [ycol (i/trans [ts])
        X (whoop-cofactor (count ts))
        V (variance-matrix X)]
  (i/sel (i/mmult V (i/trans X) ycol) 1 0)))

(defn windowed-apply 
  "apply a function [f] to a window along a sequence [xs] of length [window]"
  [f window xs]
  (map f (partition window 1 xs)))

(defn whoopbang
  "whoopbang will find the greatest OLS drop over a timeseries [ts] given 
  sub-timeseries of length [long-block].  The drops are smoothed by a moving 
  average window of length [mov-avg]."
  [ts long-block window]
  (->> (deseasonalize ts)
       (windowed-apply whoop-ols long-block)
       (windowed-apply average window)
       (reduce min)))

;; WHIZBANG
;; need to get an associated time-series for rain. the function will
;; therefore change.

(defn with-rain-cofactor
  "construct a time-series with a "
  [x]
  x)

(defn std-error
  "get the standard error from a variance matrix"
  [x]
  x)

(defn ols-coeff
  "get the trend coefficient from a time-series, given a variance matrix"
  [ts]
  (let [ycol (i/trans [ts])
        X (with-rain-cofactor (count ts))
        V (variance-matrix X)]))

(defn whiz-ols
  "extract both the OLS trend coefficient and the t-stat associated
   with the trend characteristic"
  [ts V]
  ((juxt ols-coeff whiz-ols)) ts V)


;; Hodrick-Prescott filter

(defn insert-at
  "insert list [b] into list [a] at index [idx]."
  [idx a b]
  (let [opened (split-at idx a)]
    (concat (first opened) b (second opened))))

(defn insert-into-zeros
  "insert vector [v] into a vector of zeros of total length [len]
  at index [idx]."
  [idx len v]
  (insert-at idx (repeat (- len (count v)) 0) v))

(defn hp-mat
  "create the matrix of coefficients from the minimization problem
  required to parse the trend component from a time-series of le-
  gth [T], which has to be greater than or equal to 9 periods."
  [T]
  {:pre [(>= T 9)]
   :post [(= [T T] (i/dim %))]}
  (let [first-row  (insert-into-zeros 0 T [1 -2 1])
        second-row (insert-into-zeros 0 T [-2 5 -4 1])
        inner (for [x (range (inc (- T 5)))]
                   (insert-into-zeros x T [1 -4 6 -4 1]))]
    (i/matrix
     (concat [first-row]
             [second-row]
             inner
             [(reverse second-row)]
             [(reverse first-row)]))))


(defn hp-filter
  "return a smoothed time-series, given the HP filter parameter."
  [ts lambda]
  (let [T (count ts)
        coeff-matrix (i/mult lambda (hp-mat T))
        trend-cond (i/solve (i/plus coeff-matrix (i/identity-matrix T)))]
    (i/mmult trend-cond ts)))


(defn num-years-to-milliseconds [x] (* 365 24 60 60 1000 x))
(def offset (num-years-to-milliseconds (- 2000 1970)))
(def dates
  (map (comp #(+ offset %)
             num-years-to-milliseconds
             #(/ % 12))
       (range (count ndvi))))


(def plot1 (c/time-series-plot dates (hp-filter ndvi 20)
                               :title "NDVI and The H-P Filter"
                               :x-label "Year"
                               :y-label "NDVI values"))
(c/add-lines plot1 dates ndvi)
;; (let [x dates] (c/slider #(i/set-data plot1 [x (hp-filter ndvi %)]) (range 0 11 0.1) "lambda"))


; (def whizbang ((juxt ols-coeff ols-std-error) test-ts))


; (defn make-matrix
;   [width height]
;   "Create a matrix (list of lists)"
;   (repeat width (repeat height 0)))
; 
; (defn matrix-map
;   [m func]
;   "Apply a function to every element in a matrix"
;   (map (fn [x] (map func x)) m))
