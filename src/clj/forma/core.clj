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

;; ## Backend Data Processing Queries
;;
;; The following cascalog queries provide us with a way to process raw
;; input files into bins based on various datafields, and read them
;; back out again into a hadoop cluster for later analysis.
;;
;; ### Data to Bucket
;;
;; The idea of this portion of the project is to process all input
;; data into chunks in of data MODIS sinusoidal projection, tagged
;; with metadata to identify the chunk's position and spatial and
;; temporal resolutions. We sink these tuples into Hadoop
;; SequenceFiles binned into a custom directory structure on S3,
;; designed to facilitate easy access to subsets of the data. The
;; data bins are formatted as:
;;
;;     s3n://<dataset>/<s-res>-<t-res>/<tileid>/<jobtag>/seqfile
;;     ex: s3n://ndvi/1000-32/008006/20110226T234402Z/part-00000
;;
;; `s-res` is the spatial resolution of the data, limited to `1000`,
;; `500`, and `250`. `t-res` is the temporal resolution, keyed to the
;; MODIS system of monthly, 16-day, or 8-day periods (`t-res` = `32`,
;; `16` or `8`). `tileid` is the MODIS horizontal and vertical tile
;; location, formatted as `HHHVVV`.
;;
;; As discussed in [this thread](http://goo.gl/jV4ut) on
;; cascading-user, Hadoop can't append to existing
;; SequenceFiles. Rather than read in every file, append, and write
;; back out, we decided to bucket our processed chunks by
;; `jobid`. This is the date and time, down to seconds, at which the
;; run was completed. The first run we complete will be quite large,
;; containing over 100 time periods. Subsequent runs will be monthly,
;; and will be quite small. On a yearly basis, we plan to read in all
;; tuples from every `jobid` directory, and bin them into a new
;;`jobid`. This is to limit the number of small files in the sytem.
;;
;; Note that the other way to combat the no-append issue would have
;; been to sink tuples into a deeper directory structure, based on
;; date. The downside here is that every sequencefile would be 5MB, at
;; 1km resolution. Hadoop becomes efficient when mappers are allowed
;;to deal with splits of 64MB. By keeping our sequencefiles large, we
;;take advantage of this property.

(defn jobtag
  "Generates a unique tag for a job, based on the current time."
  [] (unparse (formatters :basic-date-time-no-ms)
              (now)))

(defn modis-seqfile
  "Cascading tap to sink MODIS tuples into a directory structure based
  on dataset, temporal and spatial resolution, tileid, and a custom
  `jobtag`. Makes use of Cascading's
  [TemplateTap](http://goo.gl/txP2a)."
  [out-dir]
  (template-seqfile out-dir
                    (str "%s/%s-%s/%s/" (jobtag) "/")))

;; And, here we are, at the chunkers. These simply execute the
;; relevant subqueries, using a TemplateTap designed to sink our
;; tuples into our custom directory structure.

(defn modis-chunker
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets c-size in-dir out-dir]
  (let [source (all-files in-dir)]
    (?- (modis-seqfile out-dir)
        (h/modis-chunks source subsets chunk-size))))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res ll-res c-size tile-seq in-dir out-dir]
  (let [source (all-files in-dir)]
    (?- (modis-seqfile out-dir)
        (r/rain-chunks m-res ll-res c-size tile-seq source))))

;; ### Bucket to Cluster
;;
;; To get tuples back out of our directory structure on S3, we employ
;; Cascading's [GlobHFS] (http://goo.gl/1Vwdo) tap, along with an
;; interface tailored for datasets stored in the MODIS sinusoidal
;; projection.

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

