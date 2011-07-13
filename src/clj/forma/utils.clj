(ns forma.utils
  (:require [clojure.contrib.io :as io])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

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
  {:pre [(not (empty? coll)), (even? (count coll))]}
  [(take-nth 2 coll) (take-nth 2 (rest coll))])

(defn scale
  "Returns a collection obtained by scaling each number in `coll` by
  the supplied number `fact`."
  [factor coll]
  (for [x coll] (* x factor)))

(defn running-sum
  "Given an accumulator, an initial value and an addition function,
  transforms the input sequence into a new sequence of equal length,
  increasing for each value."
  [acc init add-func tseries]
  (first (reduce (fn [[coll last] new]
                   (let [last (add-func last new)]
                     [(conj coll last) last]))
                 [acc init]
                 tseries)))

(defn weighted-mean
  "Accepts a number of `<val, weight>` pairs, and returns the mean of
  all values with corresponding weights applied. For example:

    (weighted-avg 8 3 1 1) => 6.25"
  [& val-weight-pairs]
  {:pre [(even? (count val-weight-pairs))]}
  (float (->> (for [[x weight] (partition 2 val-weight-pairs)]
                (if  (>= weight 0)
                  [(* x weight) weight]
                  (throw (IllegalArgumentException.
                          "All weights must be positive."))))
              (reduce (partial map +))
              (apply /))))

;; ## IO Utils

(defn input-stream
  "Attempts to coerce the given argument to an InputStream, with
automatic flipping to `GZipInputStream` if appropriate for the
supplied input. see `clojure.contrib.io/input-stream` for guidance on
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
