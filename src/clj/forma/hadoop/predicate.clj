(ns forma.hadoop.predicate
  (:use cascalog.api
        clojure.contrib.java-utils
        [clojure.string :only (split)]
        [clojure.contrib.math :only (ceil)]
        [clojure.contrib.seq :only (positions)]
        [forma.matrix.utils :only (sparse-expander matrix-of)]
        [forma.source.modis :only (pixels-at-res)])
  (:require [cascalog.ops :as c]
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

;; ### Generators

(defn pixel-generator
  "Returns a cascalog generator that produces every pixel combination
  for the supplied sequence of tiles, given the supplied
  resolution. `pixel-generator` works by first writing every tuple to
  a sequencefile into the supplied directory, then returning a tap
  into its guts.

I recommend wrapping queries that use this tap with
`cascalog.io/with-fs-tmp`; for example,

    (with-fs-tmp [fs tmp-dir]
      (let [pix-tap (pixel-generator res tmp-dir tileseq)]
      (?<- (stdout)
           [?mod-h ?mod-v ... etc]
           (pix-tap ?mod-h ?mod-v ?sample ?line)
           ...)))"
  [tmp-path res tileseq]
  (let [tap (hfs-seqfile tmp-path)]
    (with-open [collector (.openForWrite tap (JobConf.))]
      (doseq [[h v] tileseq
              s (range (pixels-at-res res))
              l (range (pixels-at-res res))]
        (.add collector (Util/coerceToTuple [h v s l]))))
    tap))

;; ### Operations

(defn add-fields
  "Adds an arbitrary number of fields to tuples in a cascalog query."
  [& fields]
  (vec fields))

(defbufferop
  ^{:doc "Returns a string representation of the tuples input to this
  buffer. Useful for testing!"}
  tuples->string
  [tuples]
  [(apply str (map str tuples))])

(defmapop
  ^{:doc "Converts nested clojure vectors into an array of the
  supplied type. For example:

    (window->array [Integer/TYPE] ?chunk :> ?int-chunk)

  flattens the chunk and returns an integer array."}
  [window->array [type]]
  [window]
  [(into-array type (flatten window))])

(defbufferop
  ^{:doc "Receives 2-tuple pairs of the form `<idx, val>`, inserts each
  `val` into a sparse vector at the corresponding `idx`. The `idx` of
  the first tuple will be treated as the zero value. The first tuple
  will `missing-val` will be substituted for any missing value."}
  [sparse-expansion [start length missing-val]]
  [tuples]
  [[(sparse-expander missing-val
                     tuples
                     :start start
                     :length length)]])

(defn mangle
  "Mangles textlines connected with commas."
  [line]
  (map (fn [val]
         (try (Float. val)
              (catch Exception _ val)))
       (split line #",")))

;; ### Predicate Macros

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
         (:sort ?idx)
         (- ?idx start :> ?start-idx)
         ((c/juxt #'mod #'quot) ?start-idx length :> ?sub-idx ?split-idx)
         (sparse-expansion [0 length empty-val] ?sub-idx ?val :> ?split-vec))))

;; ### Special Functions

;; TODO: Test this in the 3d case. I don't think it's actually doing
;; what I want it to be doing. In fact, it fails in any case but 2d.
;;
;; I think we need outpos, outval, and then n + 1 other values to
;; cycle around. We should take in a map of positions <-> dimension
;; length.

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

    (sparse-windower s [\"?sample\" \"?line\" \"?val\"] 3000 0)

  to produce a new generator that would create

    (s ?mod-h ?mod-v ?window-col ?window-row ?window)"
  ([gen in-syms val split-length sparse-val]
     (sparse-windower gen in-syms val split-length split-length sparse-val))
  ([gen in-syms val split-length split-width sparse-val]
     (let [dim-vec [split-length split-width]
           swap (partial swap-syms gen (into in-syms [val]))
           [inpos nextpos outpos inval outval] (v/gen-non-nullable-vars 5)]
       (reduce #(%2 %1)
               gen
               (for [dim (range (count in-syms))
                     :let [length (dim-vec dim)
                           empty (matrix-of sparse-val dim length)
                           aggr (vals->sparsevec length empty)]]
                 (fn [src]
                   (construct (swap [nextpos outpos outval])
                              [[src :>> (swap [inpos nextpos inval])]
                               [aggr inpos inval :> outpos outval]])))))))
