(ns forma.utils
  (:require [clojure.contrib.io :as io])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(defn thrush [& args]
  (reduce #(%2 %1) args))

(defn nth-in
  "Takes a nested collection and a sequence of keys, and returns the
  value obtained by taking `nth` in turn on each level of the nested
  collection."
  [coll ks]
  (apply thrush coll (for [k ks]
                       (fn [xs] (nth xs k)))))

(defn scale
  "Returns a collection obtained by scaling each number in `coll` by
  the supplied number `fact`."
  [fact coll]
  (for [x coll] (* x fact)))

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

;; ## IO Utils

;; TODO: Update docs. Tests.
(defn input-stream
  "Attempts to coerce the given argument to an InputStream, with added
  support for gzipped files. If the input argument does point to a
  gzipped file, the default buffer will be sized to fit one NOAA
  PREC/L binary data file at 0.5 degree resolution, or 24 groups
  of `(* 360 720)` floats. To make sure that the returned
  GZIPInputStream will fully read into a supplied byte array, see
  `forma.source.rain/force-fill`."
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

;; TODO: Update docs. Tests.
(defn force-fill
  "Forces the given stream to fill the supplied buffer. In certain
  cases, such as when a GZIPInputStream doesn't have a large enough
  buffer by default, the stream simply won't load the requested number
  of bytes. We keep trying until the damned thing is full."
  [^InputStream stream buffer]
  (loop [len (count buffer), off 0]
    (let [read (.read stream buffer off len)
          newlen (- len read)
          newoff (+ off read)]
      (cond (neg? read) read
            (zero? newlen) (count buffer)
            :else (recur newlen newoff)))))
