(ns forma.hadoop.predicate
  (:use cascalog.api
        [clojure.contrib.math :only (ceil)]
        [clojure.contrib.seq :only (positions)]
        [forma.matrix.utils :only (sparse-vector)])
  (:require [cascalog.ops :as c]))

(defbufferop [sparse-vec [length missing-val]]
  {:doc "Receives 2-tuple pairs of the form `<idx, val>`, and inserts
  each `val` into a sparse vector of the supplied length at the
  corresponding `idx`. `missing-val` will be substituted for any
  missing value."}
  [tuples]
  [[(sparse-vector length missing-val tuples)]])

(defn vals->sparsevec
  "Returns an aggregating predicate macro that stitches values into a
  sparse vector with all `?val`s at `?idx`, and `empty-val` at all
  other places. Lines are divided into `splits` based on that input
  parameter. Currently, we require that `splits` divide evenly into
  `final-length`."
  [empty-val final-length splits]
  {:pre [(zero? (mod final-length splits))]}
  (let [split-length (ceil (/ final-length splits))]
    (<- [?idx ?val :> ?split-idx ?split-vec]
        (:sort ?idx)
        ((c/juxt #'mod #'quot) ?idx split-length :> ?sub-idx ?split-idx)
        (sparse-vec [split-length empty-val] ?sub-idx ?val :> ?split-vec))))
