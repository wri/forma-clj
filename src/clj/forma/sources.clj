(ns forma.sources
  (:use cascalog.api)
  (:import [forma WholeFile]
           [cascading.tuple Fields])
  (:require (cascalog [workflow :as w])))

;; ## Tap Functions
;; These help define the source tap that will slurp up files from a
;; local file, remote file, or S3 bucket.

(defn whole-file
  "Custom scheme for dealing with entire files."
  [field-names]
  (WholeFile. (w/fields field-names)))

(defn hfs-wholefile
  "Creates a tap on HDFS using the wholefile format. Guaranteed not
   to chop files up! Required for unsupported compression formats like HDF."
  [path]
  (w/hfs-tap (whole-file Fields/ALL) path))

(defmacro casca-fn
  "Expands to a function that pulls the fields v2 from hfs-wholefile,
   and returns the fields v1."
  [name doc v1 v2]
  `(defn ~name ~doc [dir#]
     (<- ~(vec v1) ((hfs-wholefile dir#) ~@v2))))

(casca-fn all-files
          "Subquery to return all files in the supplied directory."
          [?file] [?filename ?file])

(casca-fn files-with-names
          "Subquery to return all files, along with their filenames."
          [?filename ?file] [?filename ?file])