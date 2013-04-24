(ns forma.utils
  (:use [clojure.math.numeric-tower :only (round)])
  (:require [clojure.java.io :as io])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]
            [net.lingala.zip4j.core ZipFile]
            [java.io File]
            [java.util.UUID]))

;; ## Argument Validation

(defn throw-illegal [s]
  (throw (IllegalArgumentException. s)))

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
  (apply thrush coll
         (for [k ks] (fn [xs] (nth xs k)))))

(defn unweave
  "Splits a sequence with an even number of entries into two sequences
  by pulling alternating entries.

  Example usage:
    (unweave [0 1 2 3]) => [(0 2) (1 3)]"
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

(defn multiply-rows
  "multiply matrix rows (in place) by a collection"
  [coll mat]
  (map (partial map * coll) mat))

(defn weighted-mean
  "Accepts a number of `<val, weight>` pairs, and returns the mean of
  all values with corresponding weights applied.  Preconditions ensure
  that there are pairs of value and weights, and that all weights are
  greater than or equal to zero.

  Example usage:
    (weighted-avg 8 3 1 1) => 6.25"
  [& val-weight-pairs]
  {:pre [(even? (count val-weight-pairs))
         (every? #(>= % 0) (take-nth 2 (rest val-weight-pairs)))]}
  (double (->> (for [[x weight] (partition 2 val-weight-pairs)]
                 [(* x weight) weight])
               (reduce (partial map +))
               (apply /))))

(defn positions
  "Returns a lazy sequence containing the positions at which pred
   is true for items in coll."
  [pred coll]
  (for [[idx elt] (map-indexed vector coll) :when (pred elt)] idx))

(defn trim-seq
  "Trims a sequence with initial value indexed at x0 to fit within
  bottom (inclusive) and top (exclusive).

  Example usage: 
    (trim-seq 0 2 0 [4 5 6]) => [4 5]"
  [bottom top x0 seq]
  {:pre [(not (empty? seq))]}
  (->> seq
       (drop (- bottom x0))
       (drop-last (- (+ x0 (count seq)) top))))

(defn windowed-map
  "maps an input function across a sequence of windows onto a vector
  `v` of length `window-len` and offset by 1.

  Note that this does not work with some functions, such as +.
  Not sure why"
  [f window-len v]
  (pmap f (partition window-len 1 v)))

(defn average [lst] (/ (reduce + lst) (count lst)))

(defn moving-average
  "returns a moving average of windows on `lst` with length `window`"
  [window lst]
  (map average (partition window 1 lst)))

(defn idx
  "return a list of indices starting with 1 equal to the length of
  input"
  [coll]
  (vec (map inc (range (count coll)))))

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

(defn nils-ok?
  "Checks whether one can use `to-replace` and `coll` with all types
   flag in replacement functions. False if `to-replace` is `nil` or
   `coll` contains `nil`. `==` cannot be used with `nil`."
  [to-replace coll all-types]
  (if (not all-types)
    true
    (let [total-nil (count (positions nil? coll))]
      (if (or (nil? to-replace) (pos? total-nil))
        false
        true))))

(defn get-replace-vals-locs
  "Search collection for the location of bad values, and find replacement values.
   Replacements are found to the left of a bad value, starting to the immediate
   left of each bad value and ending at the first element of the collection.
   Returns a map of replacement indices and replacement values.

  If there are no good values to the left of a given bad value,
  `default` will be returned for that value.

  If `all-types` is true, equality checking will be type independent,
  using `==` instead of `=`. So a `bad-val` of -9999 will be
  functionally equivalent to -9999.0.

   Note that using the `:all-types` true keyword with a collection
   containing `nil` or a `bad-val` of `nil` will trip a precondition
   Using `==` with `nil` causes a null pointer exception."
  [bad-val coll default all-types]
  {:pre [(nils-ok? bad-val coll all-types)]}
  (let [bad-locs (if all-types
                   (positions (partial == bad-val) coll)
                   (positions (partial = bad-val) coll))]
    (zipmap bad-locs
            (for [i bad-locs]
                (if (zero? i) ;; avoids out of bounds exception of idx -1
                  default
                  (loop [j i]
                    (cond
                     (if (if all-types
                           (not (== bad-val (coll j)))
                           (not (=  bad-val (coll j))))
                       true
                       false) (coll j)                     
                     (zero? j) default ;; all bad values from start to j
                     :else (recur (dec j)))))))))

(defn replace-from-left
  "Replace all instances of `bad-val` in a collection with good
   replacement values, defined as the first good value to the left of
   a given element. The value given with the `:default`
   keyword (defaults to `nil` is used in case a suitable replacement
   cannot be found to the left (e.g. the first or first several
   elements of the vector is \"bad\"). Providing the `:all-types`
   keyword (defaults to `false`) will make the comparison with the bad
   value type dependent. In that case, -9999 and -9999.0 would be
   functionally equivalent.

   Note that using the `:all-types` true keyword with a collection
   containing `nil` or a `bad-val` of `nil` will trip a precondition
   Using `==` with `nil` causes a null pointer exception.

   Usage:
     (replace-from-left -9999 [1 -9999 3])
     ;=> (1 nil 3)

     (replace-from-left -9999 [1 -9999 -9999 3])
     ;=> (1 1 1 3)

     (replace-from-left -9999 [1 -9999 3] :default -1)
     ;=> (1 1 3)

     (replace-from-left -9999 [-9999 -9999 3] :default -1)
     ;=> (-1 -1 3)

     (replace-from-left -9999 [1 -9999 -9999.0 3] :default -1)
     ;=> (1 1 -9999.0 3)

     (replace-from-left -9999 [1 -9999.0 -9999 3] :default -1)
     ;=> (1 -9999.0 -9999.0 3)

     ;; type-independent equality checking
     (replace-from-left -9999 [1 -9999.0 -9999 3] :default -1 :all-types true)
     ;=> (1 1 1 3)

     (replace-from-left -9999.0 [1 -9999.0 -9999 3] :default -1 :all-types true)
     ;=> (1 -9999.0 -9999.0 3)

     (replace-from-left -9999.0 [-9999 -9999.0 -9999 3] :default -1 :all-types true)
     ;=> (-1 -1 -1 3)"
  [bad-val coll & {:keys [default all-types] :or {default nil
                                                  all-types false}}]
  {:pre [(nils-ok? bad-val coll all-types)]}
  (let [replace-map (get-replace-vals-locs bad-val coll default all-types)]
    (for [i (range (count coll))]
      (if (contains? (set (keys replace-map)) i)
        (get replace-map i)
        (coll i)))))

(defn replace-from-left*
  "Nest `replace-from-left` for use with Cascalog"
  [bad-val coll & {:keys [default all-types]
                   :or {default nil all-types false}}]
  [(vec (replace-from-left bad-val coll :default default :all-types all-types))])

(defn filter*
  "Wrapper for `filter` to make it safe for use with vectors in Cascalog.

   Usage:
     (let [src [[1 [2 3 2]] [3 [5 nil 6]]]]
       (??<- [?a ?all-twos]
         (src ?a ?b)
         (filter* (partial = 2) ?b :> ?all-twos)))
     ;=> ([1 [2 2]] [3 []])"
  [pred coll]
  [(vec (filter pred coll))])

(defn replace-all
  "Replace all instances of a value in a collection with a supplied
   replacement value. The `:all-types` keyword makes equality checking
   type independent.

   Note that using the `:all-types` true keyword with a collection
   containing `nil` or a `bad-val` of `nil` will trip a precondition
   Using `==` with `nil` causes a null pointer exception.

  Usage:
    (replace-all nil -9999 [1 nil 3])
    ;=> [1 -9999 3]

    (replace-all -9999 nil [1 -9999 3])
    ;=> [1 nil 3]

    (replace-all -9999.0 nil [1 -9999 3])
    ;=> [1 -9999 3]

    (replace-all -9999.0 nil [1 -9999 3] :all-types true)
    ;=> [1 nil 3]

    (replace-all -9999 nil [1 -9999.0 3] :all-types true)
    ;=> [1 nil 3]"
  [to-replace replacement coll & {:keys [all-types]
                                  :or {all-types false}}]
  {:pre [(nils-ok? to-replace coll all-types)]}
  (let [compare-func (cond
                      (nil? to-replace) (partial = to-replace)
                      all-types (partial == to-replace)
                      :else (partial = to-replace))
        idxs (positions compare-func coll)
        replacements (repeat (count idxs) replacement)]
    (if (empty? idxs)
      coll
      (apply assoc coll
             (interleave idxs replacements)))))

(defn replace-all*
  "Wrapper for `replace-all` to make it safe for use with vectors in Cascalog"
  [to-replace replacement coll & {:keys [all-types]
                                  :or {all-types false}}]
  [(vec (replace-all to-replace replacement coll :all-types all-types))])

(defn rest*
  "Wrapper for `rest` is safe for use with Cascalog"
  [coll]
  [(vec (rest coll))])

(defn map-round*
  "Round the values of a series, returning a vector safe for use with
  Cascalog."
  [series]
  [(vec (map round series))])

(defn within-tileset?
  [tile-set h v]
  (let [tile [h v]]
    (contains? tile-set tile)))

(defn arg-parser
  "Parse arg as string if appropriate, otherwise return arg."
  [arg]
  (if (string? arg)
    (read-string arg)
    arg))

(defn sorted-ts
  "Accepts a map with date keys and time series values, and returns a
  vector with the values appropriately sorted.

  Example:
    (sorted-ts {:2005-12-31 3 :2006-08-21 1}) => (3 1)"
  [m]
  (vals (into (sorted-map) m)))

(defn same-len?
  "Checks whether two collections have the same number of elements.

   Usage:
     (same-len? [1 2 3] [4 5 6])
     ;=> true"
  [coll1 coll2]
  (= (count coll1) (count coll2)))

(defn all-unique?
  "Checks whether all the elements in `coll` are unique.

   Usage:
     (all-unique? [1 2 3])
     ;=> true

     (all-unique? [1 1 2])
     ;=> false"
  [coll]
  (same-len? coll (set coll)))

(defn inc-eq?
  "Checks whether the first integer immediately preceeds the second one.

   Usage:
     (inc-eq? [1 2]) => true
     (inc-eq? [0 2]) => false
     (inc-eq? 1 2) => true
     (inc-eq? 0 2) => false"
  ([[a b]]
     (inc-eq? a b))
  ([a b]
     (= (inc a) b)))

(defn overlap?
  "Checks for collisions between keys in provided maps.

   Usage:
     (overlap? {:a 1} {:b 2})
     ;=> false

     (overlap? {:a 1 :b 2} {:b 3})
     ;=> true

     (overlap? {:2006-01-01 1 :2006-01-17 2} {2006-01-17 30})
     ;=> true"
  ([& maps]
     (let [all-ks (flatten (map keys maps))]
       (not (all-unique? all-ks)))))

(defn merge-no-overlap
  "This function shadows the built-in `merge` function, but uses a
   precondition to check for key collisions before merging maps.

   Usage:
     (merge-no-overlap {:2006-01-01 1 :2006-01-17 2} {:2006-02-02 3})
     ;=> {:2006-01-01 1 :2006-01-17 2 :2006-02-02 3}

     (merge-no-overlap {:2006-01-01 1 :2006-01-17 2} {2006-01-17 30})
     ;=> (throws AssertionError)"
  [& maps]
  {:pre [(not (apply overlap? maps))]}
  (apply merge maps))

(defn unzip
  "Unzips the supplied ZIP file into the supplied directory."
  [file dir]
  (let [zipfile (ZipFile. file)]
    (.extractAll zipfile dir)))

(defn gen-uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn ls
  [dir]
  (.listFiles (File. dir)))
