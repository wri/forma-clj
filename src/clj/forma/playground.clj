;; I'll put some stuff on a guide to clojure for the guys in
;; here. Know how to switch namespaces -- know what they mean, and
;; know that you can look up documentation for any function, the way
;; it's called inside of the current namespace. This makes looking up
;; the way a function works TRIVIAL! I think we can do this inside of
;; textmate clojure as well. See the swannodette documentation on
;; github for some nice examples, on getting started.

(ns forma.playground
  (:use cascalog.api
        forma.hadoop
        forma.reproject
        forma.sources)
  (:require (cascalog [ops :as c])))

(def age (memory-source-tap [
  ["alice" 28]
  ["bob" 33]
  ["chris" 40]
  ["david" 25]
  ["emily" 25]
  ["george" 31]
  ["gary" 28]
  ["kumar" 27]
  ["luanne" 36]
  ]))

(def small-tiles (memory-source-tap [
  [1 28]
  [4 33]
  [4 40]
  [2 25]
  [1 25]
  ]))

(defn file-count
  "Prints the total count of files in a given directory to stdout."
  [dir]
  (let [files (all-files dir)]
    (?<- (stdout) [?count]
         (files ?filename ?file)
         (c/count ?count))))

(defn add-floats
  "Returns a float array, for the purpose of stacking on to the end of
  a tuple. If the returned field works as a subquery result, we're in
  business."
  [file]
  (float-array 23 3.2))

(defn get-floats
  "Subquery to test float array serialization."
  [dir]
  (let [files (all-files dir)]
    (<- [?filename ?floats]
        (files ?filename ?file)
        (add-floats ?file :> ?floats)
        (:distinct false))))

(defn files-with-floats
  "Test of float array serialization."
  [dir]
  (let [stuff (get-floats dir)]
    (?<- (stdout) [?filename ?floats ?num]
         (stuff ?filename ?floats)
         (last ?floats :> ?num))))

(def tiles (memory-source-tap good-tiles))

(defmapop checker [h-tile] (list (vec (range 50 60))))

(defmapop fives [arg]
  (vector [8 7]))

(defmapop mapper [f coll]
  [(apply vector (map f coll))])

(defn rain-tester []
  (?<- (stdout) [?name ?period ?h-tile ?v-tile ?checked ?mapped]
       (age ?name ?period)
       (small-tiles ?h-tile ?v-tile)
       (checker ?h-tile :> ?checked)
       (fives ?v-tile :> ?fives)
       (mapper ?checked ?fives :> ?mapped)
       (identity 1 :> ?_)))