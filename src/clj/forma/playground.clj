(ns forma.playground
  (:use cascalog.api
        forma.sources)
  (:require (cascalog [ops :as c])))

(defn file-count
  "Prints the total count of files in a given directory to stdout."
  [dir]
  (let [files (all-files dir)]
    (?<- (stdout) [?count]
     (files ?file)
     (c/count ?count))))