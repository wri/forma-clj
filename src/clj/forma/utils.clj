(ns forma.utils
  (:use [clojure.math.numeric-tower :only (round expt)])
  (:require [clojure.java.io :as io])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

;; ## Argument Validation

(defn throw-illegal [s]
  (throw (IllegalArgumentException. s)))

(defn round-places
  "Rounds the supplied number to the supplied number of decimal
  points, and returns a float representation."
  [sig-figs number]
  (let [factor (expt 10 sig-figs)]
    (double (/ (round (* factor number)) factor))))

(defn strings->floats
  "Accepts any number of string representations of floats, and
  returns the corresponding sequence of floats."
  [& strings]
  (map #(Float. %) strings))

(defn between?
  "Returns true of the supplied arg `x` falls between the supplied
  `lower` and `upper` bounds (inclusive), false otherwise."
  [lower upper x]
  (and (>= x lower) (<= x upper)))

(defn thrush [& args]
  (reduce #(%2 %1) args))

(defn nth-in
  "Takes a nested collection and a sequence of keys, and returns the
  value obtained by taking `nth` in turn on each level of the nested
  collection."
  [coll ks]
  (apply thrush coll (for [k ks]
                       (fn [xs] (nth xs k)))))

(defn unweave
  "Splits a sequence with an even number of entries into two sequences
  by pulling alternating entries. For example:

    (unweave [0 1 2 3])
    ;=> [(0 2) (1 3)]"
  [coll]
  {:pre [(seq coll), (even? (count coll))]}
  [(take-nth 2 coll) (take-nth 2 (rest coll))])

(defn find-first
  "Returns the first item of coll for which (pred item) returns logical true.
  Consumes sequences up to the first match, will consume the entire sequence
  and return nil if no match is found."
  [pred coll]
  (first (filter pred coll)))

(defn scale
  "Returns a collection obtained by scaling each number in `coll` by
  the supplied number `fact`."
  [factor coll]
  (for [x coll] (* x factor)))

(defn dot-product
  "returns the dot product of two vectors"
  [x y]
  (reduce + (map * x y)))

(defn transpose
  "returns the transposition of a `coll` of vectors"
  [coll]
  (apply map vector coll))

(defn multiply-rows
  "multiply matrix rows (in place) by a collection"
  [coll mat]
  (map (partial map * coll) mat))

(defn weighted-mean
  "Accepts a number of `<val, weight>` pairs, and returns the mean of
  all values with corresponding weights applied. For example:

    (weighted-avg 8 3 1 1) => 6.25"
  [& val-weight-pairs]
  {:pre [(even? (count val-weight-pairs))]}
  (float (->> (for [[x weight] (partition 2 val-weight-pairs)]
                (if  (>= weight 0)
                  [(* x weight) weight]
                  (throw-illegal "All weights must be positive.")))
              (reduce (partial map +))
              (apply /))))

(defn positions
  "Returns a lazy sequence containing the positions at which pred
   is true for items in coll."
  [pred coll]
  (for [[idx elt] (map-indexed vector coll) :when (pred elt)] idx))

(defn trim-seq
  "Trims a sequence with initial value indexed at x0 to fit within
  bottom (inclusive) and top (exclusive). For example:

    (trim-seq 0 2 0 [1 2 3]) => [0 1 2]"
  [bottom top x0 seq]
  (->> seq
       (drop (- bottom x0))
       (drop-last (- (+ x0 (count seq)) top))))

(defn windowed-map
  "maps an input function across a sequence of windows onto a vector
  `v` of length `window-len` and offset by 1.

  Note that this does not work with some functions, such as +. Not sure why"
  [f window-len v]
  (pmap f (partition window-len 1 v)))

(defn average [lst] (/ (reduce + lst) (count lst)))

(defn moving-average
  "returns a moving average of windows on `lst` with length `window`"
  [window lst]
  (map average (partition window 1 lst)))


;; ## IO Utils

(defn input-stream
  "Attempts to coerce the given argument to an InputStream, with
automatic flipping to `GZipInputStream` if appropriate for the
supplied input. see `clojure.java.io/input-stream` for guidance on
valid arguments."
  ([arg] (input-stream arg nil))
  ([arg default-bufsize]
     (let [^InputStream stream (io/input-stream arg)]
       (try
         (.mark stream 0)
         (if default-bufsize
           (GZIPInputStream. stream default-bufsize)
           (GZIPInputStream. stream))
         (catch java.io.IOException e
           (.reset stream)
           stream)))))

;; When using an input-stream to feed lazy-seqs, it becomes difficult
;; to use `with-open`, as we don't know when the application will be
;; finished with the stream. We deal with this by calling close on the
;; stream when our lazy-seq bottoms out.

(defn force-fill!
  "Forces the contents of the supplied `InputStream` into the supplied
  byte buffer. (In certain cases, such as when a GZIPInputStream
  doesn't have a large enough buffer by default, the stream simply
  won't load the requested number of bytes. `force-fill!` blocks until
  `buffer` has been filled."
  [buffer ^InputStream stream]
  (loop [len (count buffer), off 0]
    (let [read (.read stream buffer off len)
          newlen (- len read)
          newoff (+ off read)]
      (cond (neg? read) read
            (zero? newlen) (count buffer)
            :else (recur newlen newoff)))))

(defn partition-stream
  "Generates a lazy seq of byte-arrays of size `n` pulled from the
  supplied input stream `stream`."
  [n ^InputStream stream]
  (let [buf (byte-array n)]
    (if (pos? (force-fill! buf stream))
      (lazy-seq
       (cons buf (partition-stream n stream)))
      (.close stream))))

(defn read-numbers [x]
  (binding [*read-eval* false]
    (let [val (read-string x)]
      (assert (number? val) "You can only liberate numbers!")
      val)))

;; ## Byte Manipulation

(def float-bytes
  (/ ^Integer Float/SIZE
     ^Integer Byte/SIZE))

(def byte-array-type
  (class (make-array Byte/TYPE 0)))

;; We define a function below that flips the endian order of a
;; sequence of bytes -- we can use this to coerce groups of
;; little-endian bytes into big-endian floats.

(defn flipped-endian-float
  "Flips the endian order of each byte in the supplied byte sequence,
  and converts the sequence into a float. Currently we limit the size
  of the `byte-seq` to 4."
  [byte-seq]
  {:pre [(= 4 (count byte-seq))]}
  (->> byte-seq
       (map-indexed (fn [idx bit]
                      (bit-shift-left
                       (bit-and bit 0xff)
                       (* 8 idx))))
       (reduce +)
       (Float/intBitsToFloat)))
