(ns forma.playground
  (:use cascalog.api
        forma.hadoop
        forma.sources)
  (:require (clojure.contrib.http [agent :as h])
            (cascalog [ops :as c])))

(defn file-count
  "Prints the total count of files in a given directory to stdout."
  [dir]
  (let [files (all-files dir)]
    (?<- (stdout) [?count]
     (files ?file)
     (c/count ?count))))

(defn add-floats
  "Returns a float array, for the purpose of stacking on to the end of
  a tuple. If the returned field works as a subquery result, we're in
  business."
  [file]
  (float-array 10 12))

(defn get-floats
  "Subquery to test float array serialization."
  [dir]
  (let [files (files-with-names dir)]
    (<- [?filename ?floats]
        (files ?filename ?file)
        (add-floats ?file :> ?floats)
        (:distinct false))))

(defn files-with-bytes
  "Test of float array serialization."
  [dir]
  (let [stuff (get-floats dir)]
      (?<- (stdout) [?filename ?floats ?num]
           (stuff ?filename ?floats)
           (last ?floats :> ?num))))