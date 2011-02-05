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

(defn add-bytes [file]
  (float-array 10 3.2))



(defn get-floats
  "Test of byte array serialization."
  [dir]
  (let [files (files-with-names dir)]
    (<- [?filename ?floats]
        (files ?filename ?file)
        (add-bytes ?file :> ?floats)
        (:distinct false))))

(defn files-with-bytes
  "Test of byte array serialization."
  [dir]
  (let [stuff (get-floats dir)]
      (?<- (stdout) [?filename ?floats ?num]
           (stuff ?filename ?floats)
           (last ?floats :> ?num))))