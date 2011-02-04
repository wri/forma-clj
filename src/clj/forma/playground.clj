(ns forma.playground
  (:use cascalog.api
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

(defmapop add-bytes [file]
  (byte-array 10))

(defn files-with-bytes
  "Test of byte array serialization."
  [dir]
  (let [files (all-files dir)]
    (?<- (stdout) [?bytes]
     (files ?file)
     (add-bytes ?file :> ?floats))))