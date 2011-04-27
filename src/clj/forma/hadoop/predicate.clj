(ns forma.hadoop.predicate
  (:use cascalog.api
        [clojure.string :only (split)]
        [clojure.contrib.math :only (ceil)]
        [clojure.contrib.seq :only (positions)]
        [forma.matrix.utils :only (sparse-vector matrix-of)])
  (:require [cascalog.ops :as c]
            [cascalog.vars :as v]))

;; ### Cascalog Helpers

(defn swap-syms
  "Returns a vector of dynamic variable outputs from a cascalog
  generator, with `in-syms` swapped for `out-syms`."
  [gen in-syms out-syms]
  (replace (zipmap in-syms out-syms)
           (get-out-fields gen)))

;; ### Operations

(defbufferop tuples->string
  {:doc "Returns a string representation of the tuples input to this
  buffer. Useful for testing!"}
  [tuples]
  [(apply str (map str tuples))])

(defbufferop [sparse-vec [length missing-val]]
  {:doc "Receives 2-tuple pairs of the form `<idx, val>`, and inserts
  each `val` into a sparse vector of the supplied length at the
  corresponding `idx`. `missing-val` will be substituted for any
  missing value."}
  [tuples]
  [[(sparse-vector length missing-val tuples)]])

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
  [split-length empty-val]
  (<- [?idx ?val :> ?split-idx ?split-vec]
      (:sort ?idx)
      ((c/juxt #'mod #'quot) ?idx split-length :> ?sub-idx ?split-idx)
      (sparse-vec [split-length empty-val] ?sub-idx ?val :> ?split-vec)))

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

    (sparse-windower s [\"?sample\" \"?line\" \"?val\"] 3000 0)

  to produce a new generator that would create

    (s ?mod-h ?mod-v ?window-col ?window-row ?window)"
  [gen in-syms split-length sparse-val]
  (let [swap (partial swap-syms gen in-syms)
        [inpos nextpos outpos inval outval] (v/gen-non-nullable-vars 5)]
    (reduce #(%2 %1)
            gen
            (for [dim (range (dec (count in-syms)))
                  :let [empty (matrix-of sparse-val dim split-length)
                        aggr (vals->sparsevec split-length empty)]]
              (fn [src]
                (construct (swap [nextpos outpos outval])
                           [[src :>> (swap [inpos nextpos inval])]
                            [aggr inpos inval :> outpos outval]]))))))

