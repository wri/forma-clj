(ns forma.hadoop.jobs.load-tseries
  (:use cascalog.api
        [forma.hadoop.io :only (chunk-tap)])
  (:require [cascalog.ops :as c])
  (:gen-class))

;; This is a test, to make sure that GlobHFS works. I'm happy to say
;; that it does! (Really soon, we'll need to move this and other tests
;; into test files.) This query takes a base path and various
;; arguments, as described by `(doc globstring)`, gathers all tuples
;; sunk into the corresponding subdirectories, and counts up the
;; number of chunks for each tile and dataset. The test makes it clear
;; that our system doesn't filter duplicates, so we'll have to do this
;; in code, and cull that stuff out every year, on our yearly run.

(defn read-chunks
  "Takes in a path and a number of pieces, and performs a test
  operation on all tuples matching the glob."
  [path & pieces]
  (let [source (apply chunk-tap path pieces)]
    (?<- (stdout)
         [?dataset ?tilestring ?date ?count]
         (source ?dataset ?s-res ?t-res ?tilestring ?date ?chunkid ?chunk)
         (c/count ?count))))

(defn -main
  "TODO: -- options, here!"
  [arg1]
  arg1)
