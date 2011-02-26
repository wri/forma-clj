;; This file isolates all direct interaction with Hadoop, and provides
;; access to any custom schemes used by FORMA. Any new schemes, or
;; low-level additions to our hadoop interaction capabilities should
;; go here, allowing our other components to work at a high level of abstraction.

(ns forma.hadoop
  (:use cascalog.api
        (clojure.contrib [math :only (abs)]))
  (:import [forma WholeFile]
           [cascading.tuple Fields]
           [cascading.tap TemplateTap SinkMode]
           [org.apache.hadoop.io BytesWritable])
  (:require (cascalog [workflow :as w])))

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
;; wonderful [Yahoo!  tutorial](http://goo.gl/u2hOe); "By default, the
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
;; became trivial:

(defn whole-file
  "Custom scheme for dealing with entire files."
  [field-names]
  (WholeFile. (w/fields field-names)))

;; Another helpful feature provided by cascading is the ability to
;; pair a scheme with a special [HFS class](http://goo.gl/JHpNT), that
;; allows access to a number of different file systems. A tap
;; acts like a massive vacuum cleaner. Open up a tap in a directory,
;; and the tap inhales everything inside of it, passing it in as food
;; for whatever scheme it's associated with. HFS can deal with HDFS,
;; local fileystem, and Amazon S3 bucket paths; A path prefix of, respectively,
;; hdfs://, file://, and s3:// forces the proper choice.

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

;; ## Intermediate Taps

(defn template-seqfile
  "Opens up a Cascading [TemplateTap](http://goo.gl/Vsnm5) that sinks
tuples into the supplied directory, using the format specified by
`pathstr`. Supports `:keep`, `:append` and `:replace` options for
`SinkMode`; defaults to `:append`."
  ([path pathstr]
     (template-seqfile path pathstr :append))
  ([path pathstr mode]
     (let [sinkmode (case mode
                          :keep (SinkMode/KEEP)
                          :append (SinkMode/APPEND)
                          :replace (SinkMode/REPLACE))]
     (TemplateTap. (w/hfs-tap (w/sequence-file Fields/ALL) path)
                   pathstr
                   sinkmode))))

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
  (str (abs (.hashCode bytes))))

(defn get-bytes
  "Extracts a byte array from a Hadoop BytesWritable object. As
  mentioned in the [BytesWritable javadoc](http://goo.gl/cjjlD), only
  the first N bytes are valid, where N = (.getLength byteswritable)."
  [^BytesWritable bytes]
  (byte-array (.getLength bytes)
              (.getBytes bytes)))