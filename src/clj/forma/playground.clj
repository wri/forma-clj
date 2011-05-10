;; I'll put some stuff on a guide to clojure for the guys in
;; here. Know how to switch namespaces -- know what they mean, and
;; know that you can look up documentation for any function, the way
;; it's called inside of the current namespace. This makes looking up
;; the way a function works TRIVIAL! I think we can do this inside of
;; textmate clojure as well. See the swannodette documentation on
;; github for some nice examples, on getting started.

(ns forma.playground
  (:use cascalog.api
        cascalog.playground
        forma.hadoop.io)
  (:require [incanter.core :as i]
            [cascalog.ops :as c]
            [clojure.string :as s])
  (:gen-class
   :name  testing.dyn
   :extends javax.swing.JFrame
   :implements [clojure.lang.IMeta]
   :prefix df-
   :state state
   :init  init
   :constructors {[String] [String]}
   :methods [[display [java.awt.Container] void]
             ^{:static true} [version [] String]])
  (:import (javax.swing JFrame JPanel)
           (java.awt BorderLayout Container)))

(defn df-meta [this] @(.state this))
(defn version [] "1.0")
(defn df-display [this pane]
  (doto this
    (-> .getContentPane .removeAll)
    (.setContentPane (doto (JPanel.)
                       (.add pane BorderLayout/CENTER)))
    (.pack)
    (.setVisible true)))

(defn file-count
  "Prints the total count of files in a given directory to stdout."
  [dir]
  (let [files (hfs-wholefile dir)]
    (?<- (stdout) [?count]
         (files ?filename ?file)
         (c/count ?count))))

(defn text->num
  "converts a text line of numbers to a float vectors; 
    skips the variable #skip of elements"
  [txtln]
  [(map #(Float. %)
        (drop 1 (s/split txtln #" ")))])

(defn time-cofactors
  "creates a pd x 2 matrix of ones and incremental timeseries"
  [pd]
  (let [ones (i/trans (repeat pd 1))
        ind (i/trans (range 1 (inc pd)))]
    (i/bind-columns ones ind)))

(defn ols-coefficient
  "OLS timeseries regression on a sequence m, with intercept"
  [m pd]
  (let [y-col (i/trans [m])
        X (time-cofactors pd)
        ssX (i/solve (i/mmult (i/trans X) X))]
    [(i/sel (i/mmult ssX (i/trans X) y-col) 1 0)]))

(defn wordcount 
    [path pd]
    (?<- (stdout) [?sum]
        ((hfs-textline path) ?line)
        (text->num ?line :> ?vector)
        (ols-coefficient ?vector pd :> ?sum)))

(defn globfile-test
  [pattern]
  (let [source (globhfs-wholefile pattern)]
    (?<- (stdout) [?filename]
         (source ?filename ?file))))
