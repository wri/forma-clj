;; This namespace is a hub for all others, and not really complete in
;; its own right. We define various forma constants, and hold some of
;; the test queries in here for now. Some of these bad boys need to
;; get moved over to tests.

(ns forma.core
  (:use cascalog.api
        (clj-time [format :only (unparse formatters)]
                  [core :only (now)])
        (forma [hadoop :only (all-files
                              template-seqfile
                              globhfs-seqfile)]))
  (:require (cascalog [ops :as c])
            (forma [hdf :as h]
                   [rain :as r])))

;; ### FORMA Constants
;;
;; Aside from our EC2 information, this is the only data we need to
;; supply for the first phase of operations. Everything else should be
;; able to remain local to individual modules.

(def forma-subsets #{:ndvi :evi :qual :reli})

;; Arbitrary number of pixels slurped at a time off of a MODIS raster
;; band. For 1km data, each MODIS tile is 1200x1200 pixels; dealing
;; with each pixel individually would incur unacceptable IO costs
;; within hadoop. We currently fix the chunk size at 24,000, resulting
;; in 60 chunks per 1km data. Sharper resolution -> more chunks!

(def chunk-size 24000)

;; ### Demonstration Queries

;; TODO -- document
(defn jobtag
  "Generates a unique tag for a job, based on the current time."
  [] (unparse (formatters :basic-date-time-no-ms)
              (now)))

;; TODO -- document
(defn modis-seqfile
  "oc"
  [out-dir]
  (template-seqfile out-dir
                    (str "%s/%s-%s/%s/" (jobtag) "/")))

(defn chunk-test
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets c-size in-dir out-dir]
  (let [source (all-files in-dir)]
    (?- (modis-seqfile out-dir)
        (h/modis-chunks source subsets chunk-size))))

(defn rain-test
  "Like chunk-test, but for NOAA PRECL data files."
  [m-res ll-res c-size tile-seq in-dir out-dir]
  (let [source (all-files in-dir)]
    (?- (modis-seqfile out-dir)
        (r/rain-chunks m-res ll-res c-size tile-seq source))))

;; TODO -- come up with a better syntax! Also, talk about how we make
;; this puppy work.
(defn globstring
  "Takes a path ending in `/` and collections of datasets,
  resolutions, and tiles, and returns a globstring formatted for
  cascading's GlobHFS. (`*` may be substituted in for any argument but
  path.)"
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

(defn read-test
  "Takes in a path and a set of pieces, and performs a test operation
  on all tuples matching the glob."
  [path & pieces]
  (let [source (globhfs-seqfile (apply globstring path pieces))]
    (?<- (stdout)
         [?dataset ?tilestring ?date ?count]
         (source ?dataset ?s-res ?t-res ?tilestring ?date ?chunkid ?chunk)
         (c/count ?count))))