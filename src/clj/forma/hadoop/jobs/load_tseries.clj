(ns forma.hadoop.jobs.load-tseries
  (:use cascalog.api
        [forma.hadoop.io :only (globhfs-seqfile)])
  (:require [cascalog.ops :as c])
  (:gen-class))

;; ### Bucket to Cluster
;;
;; To get tuples back out of our directory structure on S3, we employ
;; Cascading's [GlobHFS] (http://goo.gl/1Vwdo) tap, along with an
;; interface tailored for datasets stored in the MODIS sinusoidal
;; projection. For details on the globbing syntax, see
;; [here](http://goo.gl/uIEzu).

(defn globstring
  "Takes a path ending in `/` and collections of datasets,
  resolutions, and tiles, and returns a globstring formatted for
  cascading's GlobHFS. (`*` may be substituted in for any argument but
  path.)

    Example Usage:
    (globstring \"s3://bucket/\" [\"ndvi\" \"evi\"] [\"1000-32\"] *)
    ;=> \"s3://bucket/{ndvi,evi}/{1000-32}/*/*/\"

    (globstring \"s3://bucket/\" * * [\"008006\" \"033011\"])
    ;=> \"s3://bucket/*/*/{008006,033011}/*/\""
  ([basepath datasets resolutions tiles]
     (globstring basepath datasets resolutions tiles *))
  ([basepath datasets resolutions tiles batches]
     (letfn [(wrap [coll]
               (format "{%s}/"
                       (apply str (interpose "," coll))))
             (bracketize [arg]
               (if (= * arg) "*/" (wrap arg)))]
       (apply str
              basepath
              (map bracketize
                   [datasets resolutions tiles batches])))))

;; This is a test, to make sure that GlobHFS works. I'm happy to say
;; that it does! (Really soon, we'll need to move this and other tests
;; into test files.) This query takes a base path and various
;; arguments, as described by `(doc globstring)`, gathers all tuples
;; sunk into the corresponding subdirectories, and counts up the
;; number of chunks for each tile and dataset. The test makes it clear
;; that our system doesn't filter duplicates, so we'll have to do this
;; in code, and cull that stuff out every year, on our yearly run.

(defn read-test
  "Takes in a path and a number of pieces, and performs a test
  operation on all tuples matching the glob."
  [path & pieces]
  (let [source (globhfs-seqfile (apply globstring path pieces))]
    (?<- (stdout)
         [?dataset ?tilestring ?date ?count]
         (source ?dataset ?s-res ?t-res ?tilestring ?date ?chunkid ?chunk)
         (c/count ?count))))

(defn -main
  "TODO: -- options, here!"
  [arg1]
  arg1)
