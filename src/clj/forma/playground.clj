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
        forma.modis
        forma.reproject)
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

(def tiles (memory-source-tap (vec valid-tiles)))

(defmapop checker [h-tile] (list (vec (range 50 60))))

(defmapop fives [arg]
  (vector [8 7]))

(defmapop fancyvec [coll indices]
  [(vec (map (vec coll) indices))])

(defmapop fancy [coll indices]
  [(map (vec coll) indices)])

(defn rain-tester []
  (?<- (stdout) [?name ?period ?h-tile ?v-tile ?checked ?mapped]
       (age ?name ?period)
       (small-tiles ?h-tile ?v-tile)
       (checker ?h-tile :> ?checked)
       (fives ?v-tile :> ?fives)
       (fancyvec ?checked ?fives :> ?mapped)
       (identity 1 :> ?_)))


;; Okay, we know that this will give us an array to test stuff on.
(def tester
  (byte-array
   (flatten
    (take 10
          (repeatedly
           #(map byte [0 -64 121 -60]))))))

(defmacro mapped-case
  [elt]
  (cons 'case
        (cons elt
              (for [[test val] {1 1000 2 500 4 250}]
                test val))))