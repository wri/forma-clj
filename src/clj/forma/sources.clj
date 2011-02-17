;; ## TODO -- information about the sources for data, here, and how
;; ## we're supplementing the cascalog sources.2

(ns forma.sources
  (:use cascalog.api)
  (:import [forma WholeFile]
           [cascading.tuple Fields])
  (:require (cascalog [workflow :as w])))

;; ## Tap Functions
;; These define the source tap that will slurp up files from a local
;; file, remote file, or S3 bucket. See documentation on Cascading's
;; HFS scheme for more information:
;; http://www.cascading.org/1.2/javadoc/cascading/tap/Hfs.html

(defn whole-file
  "Custom scheme for dealing with entire files."
  [field-names]
  (WholeFile. (w/fields field-names)))

(defn hfs-wholefile
  "Creates a tap on HDFS using the wholefile format. Guaranteed not to
   chop files up! Required for unsupported compression formats like
   HDF."
  [path]
  (w/hfs-tap (whole-file Fields/ALL) path))

(defn all-files
  "Subquery to return all files in the supplied directory. Files will
  be returned as 2-tuples, formatted as (filename, file) The filename
  is a text object, while the file is encoded as a Hadoop
  BytesWritable."
  [dir]
  (let [source (hfs-wholefile dir)]
    (<- [?filename ?file]
        (source ?filename ?file))))