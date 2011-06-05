;; This file isolates all direct interaction with Hadoop, and provides
;; access to any custom schemes used by FORMA. Any new schemes, or
;; low-level additions to our hadoop interaction capabilities should
;; go here, allowing our other components to work at a high level of
;; abstraction.

(ns forma.hadoop.io
  (:use cascalog.api
        [forma.date-time :only (jobtag)]
        [clojure.string :only (join)]
        [forma.source.modis :only (valid-modis?)])
  (:require [cascalog.workflow :as w])
  (:import [forma WholeFile]
           [forma.schema DoubleArray IntArray]
           [java.util ArrayList]
           [cascading.tuple Fields]
           [cascading.scheme Scheme]
           [cascading.tap TemplateTap SinkMode GlobHfs]
           [org.apache.hadoop.io BytesWritable]))

;; ## Custom File Input
;;
;; Hadoop is optimized for massive files -- enormous website logs,
;; huge bodies of text, lists of friend relations on twitter, that
;; sort of stuff. Hadoop jobs require a defined input format for each
;; job, [as described here](http://goo.gl/YX2Ol). The input format
;; generates a number of [Input Splits](http://goo.gl/0UWKd); these
;; input splits are byte streams, and require a custom [record
;; reader](http://goo.gl/AmVJB) for conversion to key-value pairs. The
;; text record reader, for example, would convert a 64MB chunk of
;; lines into <k, v> pairs of <byte-offset, line> within the actual
;; input split. Most word counting examples ignore this key.
;;
;; The data from each input split goes to a single mapper. From the
;; wonderful [Yahoo! tutorial](http://goo.gl/u2hOe); "By default, the
;; FileInputFormat and its descendants break a file up into 64 MB
;; chunks (the same size as blocks in HDFS)." By splitting a file on
;; block size, Hadoop allows processing on huge documents to scale
;; linearly. Twice as many bytes flows into twice as many mappers, so
;; doubling the nodes should keep processing time the same.
;;
;; Now, this is all well and good for massive text files; as long as
;; one doesn't split in the middle of a line, the sizes of the chunks
;; are arbitrary. With certain compressed file formats, the picture
;; changes a bit. NASA's MODIS files are compressed using HDF4. A cut
;; in the middle of one of these files would result in meaningless
;; data, as neither half can be decompressed without the other.
;;
;; To deal with this issue, we've written a custom WholeFile input
;; format, to guarantee that input splits will break along file
;; boundaries. The custom WholeFileRecordReader takes one of these
;; input splits and converts it into a <k, v> pair of <filename,
;; file-bytes>. The file's bytes are wrapped in a Hadoop
;; [BytesWritable](http://goo.gl/GVP3e) object. (It should be noted
;; that for small files, this method will be very inefficient, as
;; we're overriding Hadoop's ability to leap across files and stick to
;; its optimal 64MB input split size.)
;;
;;MODIS files range from 24 to 48 MB, so all is well, but a more
;; efficient general solution can be obtained by deriving
;; WholeFileInputFormat from
;; [CombineFileInputFormat](http://goo.gl/awr4T). In this scenario, An
;; input split would still break along file boundaries, but might
;; include multiple files. The record reader would have to be adjusted
;; to produce multiple <k,v> pairs for every input split.
;;
;; As [Cascalog](http://goo.gl/SRmDh) allows us to write cascading
;; jobs (rather than straight MapReduce), we had to define a custom
;; WholeFile [scheme](http://goo.gl/Doggg) to be able to take
;; advantage of [cascading's Tap abstraction](http://goo.gl/RNMLT) for
;; data sources. Once we had WholeFile.java, the clojure wrapper
;; became trivial.

;; ## Cascading Schemes

(defn whole-file
  "Custom scheme for dealing with entire files."
  [field-names]
  (WholeFile. (w/fields field-names)))

;; ## Cascading Taps
;;
;; Another helpful feature provided by cascading is the ability to
;; pair a scheme with a special [HFS class](http://goo.gl/JHpNT), that
;; allows access to a number of different file systems. A tap
;; acts like a massive vacuum cleaner. Open up a tap in a directory,
;; and the tap inhales everything inside of it, passing it in as food
;; for whatever scheme it's associated with. HFS can deal with HDFS,
;; local fileystem, and Amazon S3 bucket paths; A path prefix of, respectively,
;; `hdfs://`, `file://`, and `s3n://` forces the proper choice.

(defn template-tap [^Scheme scheme path-or-file pathstr]
  (TemplateTap. (w/hfs-tap scheme (w/path path-or-file))
                pathstr))

(defn template-seqfile
  "Opens up a Cascading [TemplateTap](http://goo.gl/Vsnm5) that sinks
tuples into the supplied directory, using the format specified by
`pathstr`."
  [path pathstr]
  (template-tap (w/sequence-file Fields/ALL) path pathstr))

(defn hfs-wholefile
  "Subquery to return distinct files in the supplied directory. Files
  will be returned as 2-tuples, formatted as `<filename, file>` The
  filename is a text object, while the entire, unchopped file is
  encoded as a Hadoop `BytesWritable` object."
  [path]
  (w/hfs-tap (whole-file Fields/ALL) path))

(defn globhfs-wholefile
  "Subquery to return distinct files in the supplied directory that
  match the supplied pattern. See [this link](http://goo.gl/uIEzu) for
  details on Hadoop's globbing pattern syntax.

  As with `hfs-wholefile`, files will be returned as 2-tuples,
  formatted as `<filename, file>` The filename is a text object, while
  the entire, unchopped file is encoded as a Hadoop `BytesWritable`
  object."
  [pattern]
  (GlobHfs. (whole-file Fields/ALL) pattern))

(defn globhfs-seqfile
  "Identical tap to `globhfs-wholefile`, to be used with
  `SequenceFile`s instead of entire files."
  [pattern]
  (GlobHfs. (w/sequence-file Fields/ALL) pattern))

(defn globhfs-textline
  "Identical tap to `globhfs-wholefile`, to be used with text files."
  [pattern]
  (GlobHfs. (w/text-line ["line"] Fields/ALL) pattern))

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

;; ### Bucket to Cluster
;;
;; To get tuples back out of our directory structure on S3, we employ
;; Cascading's [GlobHFS] (http://goo.gl/1Vwdo) tap, along with an
;; interface tailored for datasets stored in the MODIS sinusoidal
;; projection. For details on the globbing syntax, see
;; [here](http://goo.gl/uIEzu).

(defn tiles->globstring
  [& tiles]
  {:pre [(valid-modis? tiles)]}
  (->> (for [[th tv] tiles]
         (format "h%02dv%02d" th tv))
       (join "," )
       (format "*{%s}*")))

(defn globstring
  "Takes a path ending in `/` and collections of nested
  subdirectories, and returns a globstring formatted for cascading's
  GlobHFS. (`*` may be substituted in for any argument but path.)

    Example Usage:
    (globstring \"s3n://bucket/\" [\"ndvi\" \"evi\"] [\"1000-32\"] *)
    ;=> \"s3://bucket/{ndvi,evi}/{1000-32}/*/\"

    (globstring \"s3n://bucket/\" * * [\"008006\" \"033011\"])
    ;=> \"s3://bucket/*/*/{008006,033011}/\""
  [basepath & pieces]
  (let [globber (fn [arg]
                  (if (= * arg)
                    "*/"
                    (format "{%s}/" (join ","
                                          (if (coll? arg) arg
                                              [arg]))))) ]
    (->> pieces
         (map globber)
         (apply str basepath))))

(defn chunk-tap
  "Generalized source and sink for the chunk tuples stored and
  processed by the FORMA system. The source is a cascading tap that
  sinks MODIS tuples into a directory structure based on dataset,
  temporal and spatial resolution, tileid, and a custom `jobtag`. The
  `chunk-tap`source makes use of Cascading's
  [TemplateTap](http://goo.gl/txP2a).

  The sink makes use of `globhfs-seqfile` to draw tuples back out of
  the directory structure using a globstring constructed out of the
  supplied basepath and collections of datasets, resolutions, tiles
  and specific data runs, identified by jobtag."
  ([out-dir]
     (template-seqfile out-dir
                       (str "%s/%s-%s/%s/" (jobtag) "/")))
  ([basepath datasets resolutions]
     (chunk-tap basepath datasets resolutions * *))
  ([basepath datasets resolutions tiles]
     (chunk-tap basepath datasets resolutions tiles *))
  ([basepath datasets resolutions tiles batches]
     (globhfs-seqfile (globstring basepath datasets
                                  resolutions
                                  tiles batches))))

;; ## BytesWritable Interaction
;;
;; For schemes that specifically deal with Hadoop BytesWritable
;; objects, we provide the following methods to abstract away the
;; confusing java details. (For example, while a BytesWritable object
;; wraps a byte array, not all of the bytes returned by the getBytes
;; method are valid. As mentioned in the
;; [documentation](http://goo.gl/3qzyc), "The data is only valid
;; between 0 and getLength() - 1.")

(defn hash-str
  "Generates a unique identifier for the supplied BytesWritable
  object. Useful as a filename, when worried about clashes."
  [^BytesWritable bytes]
  (-> bytes .hashCode Math/abs str))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object. As
  mentioned in the [BytesWritable javadoc](http://goo.gl/cjjlD), only
  the first N bytes are valid, where N = `(.getLength byteswritable)`."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))

;; ## Thrift Wrappers

(defn list-of
  "Maps `f` across all entries in `xs`, and returns the result wrapped
  in an instance of `java.util.ArrayList`."
  [f xs]
  (ArrayList. (for [x xs]
                (try (f x)
                     (catch Exception e nil)))))

(defn int-struct
  "Casts all numbers in the supplied sequence to ints, and returns
  them wrapped in an instance of `forma.schema.IntArray`."
  [xs]
  (let [ints (list-of int xs)]
    (doto (IntArray.)
      (.setInts ints))))

(defn double-struct
  "Casts all numbers in the supplied sequence to doubles, and returns
  them wrapped in an instance of `forma.schema.DoubleArray`."
  [xs]
  (let [doubles (list-of double xs)]
    (doto (DoubleArray.)
      (.setDoubles doubles))))

(defprotocol Thriftable
  (get-vals [x])
  (count-vals [x])
  (to-struct [xs]))

(extend-protocol Thriftable
  java.lang.Iterable
  (to-struct [[v :as xs]]
    (cond (integer? v) (int-struct xs)
          (float? v) (double-struct xs)))
  IntArray
  (get-vals [x] (.getInts x))
  (count-vals [x]
    (count (.getInts x)))
  DoubleArray
  (get-vals [x] (.getDoubles x))
  (count-vals [x]
    (count (.getDoubles x))))
