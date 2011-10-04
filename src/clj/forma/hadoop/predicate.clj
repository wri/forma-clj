(ns forma.hadoop.predicate
  (:use cascalog.api
        [juke.matrix.utils :only (sparse-expander matrix-of)]
        [juke.reproject :only (pixels-at-res)])
  (:require [juke.utils :as u]
            [forma.hadoop.io :as io]
            [clojure.string :as s]
            [cascalog.ops :as c]
            [cascalog.vars :as v])
  (:import [org.apache.hadoop.mapred JobConf]
           [cascalog Util]))

;; ### Cascalog Helpers

(defn swap-syms
  "Returns a vector of dynamic variable outputs from a cascalog
  generator, with `in-syms` swapped for `out-syms`."
  [gen in-syms out-syms]
  (replace (zipmap in-syms out-syms)
           (get-out-fields gen)))

;; ### Operations

;; #### Defmapops

(defn add-fields
  "Adds an arbitrary number of fields to tuples in a cascalog query."
  [& fields]
  (vec fields))

(defmapop [mangle [re]]
  "Splits textlines using the supplied regex."
  [line]
  (s/split line re))

;; TODO: Note that this currently forces int-structs on everything. Do
;; we want to have some way to choose?
(defn liberate
  "Takes a line with an index as the first value and numbers as the
  rest, and converts it into a 2-tuple formatted as `[idx, row-vals]`,
  where `row-vals` are sealed inside an instance of
  `forma.schema.IntArray`.

  Example usage:

    (liberate \"1 12 13 14 15\")
    ;=> [1 #<IntArray IntArray(ints:[12, 13, 14, 15])>"
  [line]
  (let [[idx & row-vals] (map u/read-numbers
                              (s/split line #"\s+"))]
    [idx (io/int-struct row-vals)]))

(defmapop [window->array [type]]
  "Converts nested clojure vectors into an array of the
  supplied type. For example:

    (window->array [Integer/TYPE] ?chunk :> ?int-chunk)

  flattens the chunk and returns an integer array."
  [window]
  [(into-array type (flatten window))])

(defmapop [window->struct [type]]
  "Converts a window of nested clojure vectors into an Thrift
  struct object designed to hold numbers of the supplied
  type. Supported types are `:int` and `:double`. For example:

    (window->struct [:int] ?chunk :> ?int-chunk)

  flattens the chunk and returns an instance of
  `forma.schema.IntArray`."
  [window]
  (let [wrap (case type
                   :double io/double-struct
                   :int io/int-struct)]
    [(-> window flatten wrap)]))

;; #### Defmapcatops

(defmapcatop index
  "splits a sequence of values into nested 2-tuples formatted
  as `<idx, val>`. Indexing is zero based."
  [xs]
  (map-indexed vector xs))

;; #### Aggregators

(defbufferop [sparse-expansion [start length missing-val]]
  "Receives 2-tuple pairs of the form `<idx, val>`, inserts each
  `val` into a sparse vector at the corresponding `idx`. The `idx` of
  the first tuple will be treated as the zero value. The first tuple
  will `missing-val` will be substituted for any missing value."
  [tuples]
  [[(sparse-expander missing-val
                     tuples
                     :start start
                     :length length)]])

(defmacro defpredsummer
  "Generates cascalog defaggregateops for counting items that satisfy
  some custom predicate. Defaggregateops don't allow anonymous
  functions, so we went this route instead."
  [name vals pred]
  `(defaggregateop ~name
     ([] 0)
     ([count# ~@vals] (if (~pred ~@vals)
                      (inc count#)
                      count#))
     ([count#] [count#])))

(defpredsummer [filtered-count [limit]]
  [val] #(> % limit))

(defpredsummer [bi-filtered-count [lim1 lim2]]
  [val1 val2] #(and (> %1 lim1)
                    (> %2 lim2)))

(defpredsummer full-count
  [val] identity)

(defmapcatop struct-index
  [idx-0 struct]
  (map-indexed (fn [idx val]
                 [(+ idx idx-0) val])
               (io/get-vals struct)))


;; ### Predicate Macros

;; TODO: Convert to dynamically opening business with predmacro.

(def ^{:doc "Converts between a textline with two numbers encoded as
strings and their integer representations."}
  converter
  (<- [?textline :> ?country ?admin]
      (mangle #"," ?textline :> ?country ?admin-s)
      (u/strings->ints ?admin-s :> ?admin)))

(def blossom-chunk
  (<- [?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?val]
      ((c/juxt #'io/extract-chunk-value
               #'io/extract-location) ?chunk :> ?static-chunk ?location)
      (struct-index 0 ?static-chunk :> ?pix-idx ?val)
      (io/expand-pos ?location ?pix-idx :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn chunkify [chunk-size]
  (<- [?dataset !date ?s-res ?t-res ?mh ?mv ?chunkid ?chunk :> ?datachunk]
      (io/chunk-location ?s-res ?mh ?mv ?chunkid chunk-size :> ?location)
      (io/mk-data-value ?chunk :> ?data-val)
      (io/mk-chunk ?dataset ?t-res !date ?location ?data-val :> ?datachunk)))

(def
  ^{:doc "Takes a source of textlines representing rows of a gridded
  dataset (with indices prepended onto each row), and generates a
  source of `row`, `col` and `val`."}
  break
  (<- [?line :> ?row ?col ?val]
      (liberate ?line :> ?row ?row-struct)
      (struct-index 0 ?row-struct :> ?col ?val)))

(defn vals->sparsevec
  "Returns an aggregating predicate macro that stitches values into a
  sparse vector with all `?val`s at `?idx`, and `empty-val` at all
  other places. Lines are divided into `splits` based on that input
  parameter. Currently, we require that `splits` divide evenly into
  `final-length`."
  ([empty-val]
     (vals->sparsevec 0 nil empty-val))
  ([length empty-val]
     (vals->sparsevec 0 length empty-val))
  ([start length empty-val]
     (<- [?idx ?val :> ?split-idx ?split-vec]
         (:sort ?sub-idx)
         (- ?idx start :> ?start-idx)
         ((c/juxt #'mod #'quot) ?start-idx length :> ?sub-idx ?split-idx)
         (sparse-expansion [0 length empty-val] ?sub-idx ?val :> ?split-vec))))

;; ### Generators

;; TODO: write a macro that generalizes this business. We can take
;; bindings like doseq and get it DONE.

(defn lazy-generator
  "Returns a cascalog generator on the supplied sequence of
  tuples. `lazy-generator` serializes each item in the lazy sequence
  into a sequencefile located at the supplied temporary directory, and
  returns a tap into its guts.

I recommend wrapping queries that use this tap with
`cascalog.io/with-fs-tmp`; for example,

    (with-fs-tmp [_ tmp-dir]
      (let [lazy-tap (pixel-generator tmp-dir lazy-seq)]
      (?<- (stdout)
           [?field1 ?field2 ... etc]
           (lazy-tap ?field1 ?field2)
           ...)))"
  [tmp-path lazy-seq]
  {:pre [(coll? (first lazy-seq))]}
  (let [tap (:sink (hfs-seqfile tmp-path))
        n-fields (count (first lazy-seq))]
    (with-open [collector (.openForWrite tap (JobConf.))]
      (doseq [item lazy-seq]
        (.add collector (Util/coerceToTuple item))))
    (name-vars tap (v/gen-non-nullable-vars n-fields))))

(defn pixel-generator
  "Returns a cascalog generator that produces every pixel combination
  for the supplied sequence of tiles, given the supplied
  resolution. `pixel-generator` stages each tuple into a sequence file
  located at `tmp-dir`. See `forma.hadoop.predicate/lazy-generator`
  for usage advice."
  [tmp-path res tileseq]
  (let [tap (:sink (hfs-seqfile tmp-path))]
    (with-open [collector (.openForWrite tap (JobConf.))]
      (doseq [[h v] tileseq
              s (range (pixels-at-res res))
              l (range (pixels-at-res res))]
        (.add collector (Util/coerceToTuple [h v s l]))))
    (name-vars tap (v/gen-non-nullable-vars 4))))

;; ### Special Functions

(defn sparse-windower
  "Aggregates cascalog generator values up into multidimensional
  windows of nested vectors of `split-length` based on the supplied
  vector of spatial variables. Any missing values will be filled with
  `sparse-val`. This is useful for aggregating spatial data into
  windows or chunks suitable for storage, or to scan across for
  nearest neighbors.

  `in-syms` should be a vector containing the name of each spatial
  dimension variable, plus the name of the value to be aggregated. For
  example, with some source `s` that would generate

    (s ?mod-h ?mod-v ?col ?row ?val)

  one could wrap s like so:

    (sparse-windower s [\"?sample\" \"?line\"] 3000 \"?val\" 0)

  to produce a new generator that would create

    (s ?mod-h ?mod-v ?window-col ?window-row ?window)"
  [gen in-syms dim-vec val sparse-val]
  (let [[outpos outval] (v/gen-nullable-vars 2)
        dim-vec (if (coll? dim-vec) dim-vec [dim-vec])
        get-length #(try (dim-vec %) (catch Exception e (last dim-vec)))]
    (apply u/thrush
           gen
           (for [[dim inpos] (map-indexed vector in-syms)
                 :let [empty-val (->> (get-length (max 0 (dec dim)))
                                      (matrix-of sparse-val dim))
                       aggr (vals->sparsevec (get-length dim) empty-val)]]
             (fn [src]
               (construct (swap-syms gen [inpos val] [outpos outval])
                          [[src :>> (get-out-fields gen)]
                           [aggr inpos val :> outpos outval]
                           [:distinct false]]))))))
