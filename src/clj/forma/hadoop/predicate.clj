(ns forma.hadoop.predicate
  (:use cascalog.api
        [clojure.contrib.math :only (ceil)]
        [clojure.contrib.seq :only (positions)]
        [forma.matrix.utils :only (sparse-vector matrix-of)])
  (:require [cascalog.ops :as c]
            [cascalog.vars :as v]))

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

(defn vals->sparsevec
  "Returns an aggregating predicate macro that stitches values into a
  sparse vector with all `?val`s at `?idx`, and `empty-val` at all
  other places. Lines are divided into `splits` based on that input
  parameter. Currently, we require that `splits` divide evenly into
  `final-length`."
  [final-length splits empty-val]
  {:pre [(zero? (mod final-length splits))]}
  (let [split-length (ceil (/ final-length splits))]
    (<- [?idx ?val :> ?split-idx ?split-vec]
        (:sort ?idx)
        ((c/juxt #'mod #'quot) ?idx split-length :> ?sub-idx ?split-idx)
        (sparse-vec [split-length empty-val] ?sub-idx ?val :> ?split-vec))))

;; ### Multidimensional Aggregation
;;
;; Okay, the next function down the line is a bit
;; wacky. `vals->sparsevec` works great at one dimensional
;; aggregation. It's a big, big bonus that this is a predicate macro,
;; since it can be composed with other queries.
;;
;; We run into a bit of trouble when we attempt 2-dimensional
;; aggregation, as aggregators can't be chained. For chunk reconstitution, .
;;
;; The following query builds more queries like this one:

(defn window-aggregator
  "Accepts a that generate tuples with fields similar to `[?mod-h
?mod-v ?sample ?line]`, and aggregates into windows across multiple
dimensions."
  ([point-source] (window-aggregator point-source 20 4))
  ([point-source edge splits]
     (let [sample-agger (vals->sparsevec edge splits 0)
           line-agger  (vals->sparsevec edge
                                        splits
                                        (matrix-of 0 1 (/ edge splits)))
           line-source (<- [?mod-h ?mod-v ?line ?window-col ?line-vec]
                           (point-source ?mod-h ?mod-v ?sample ?line ?val)
                           (sample-agger ?sample ?val :> ?window-col ?line-vec))]
       (<- [?mod-h ?mod-v ?window-col ?window-row ?window]
           (line-source ?mod-h ?mod-v ?line ?window-col ?line-vec)
           (line-agger ?line ?line-vec :> ?window-row ?window)))))

;; simplify with
;; https://github.com/nathanmarz/cascalog-workshop/blob/master/src/clj/workshop/dynamic.clj#L35
(defn build-windows
  [gen in-syms edge splits empty-val]
  (let [split-width (/ edge splits)
        [inpos nextpos outpos inval outval] (v/gen-non-nullable-vars 5)
        sym-swap  #(replace (zipmap in-syms %) (get-out-fields gen))]
    (reduce #(%2 %1)
            gen
            (for [dim (range (dec (count in-syms)))
                  :let [empty-val (matrix-of empty-val dim split-width)
                        aggr (vals->sparsevec edge splits empty-val)]]
              (fn [src]
                (construct (sym-swap [nextpos outpos outval])
                           [[src :>> (sym-swap [inpos nextpos inval])]
                            [aggr inpos inval :> outpos outval]]))))))

(defn swap-syms
  "Returns a vector of dynamic variable outputs from a cascalog
  generator, with `in-syms` swapped for `out-syms`."
  [gen in-syms out-syms]
  (replace (zipmap in-syms out-syms)
           (get-out-fields gen)))

(defn build-windows
  [gen in-syms edge splits empty-val]
  (let [split-width (/ edge splits)
        swap (partial swap-syms gen in-syms)
        [inpos nextpos outpos inval outval] (v/gen-non-nullable-vars 5)]
    (reduce #(%2 %1)
            gen
            (for [dim (range (dec (count in-syms)))
                  :let [empty-val (matrix-of empty-val dim split-width)
                        aggr (vals->sparsevec edge splits empty-val)]]
              (fn [src]
                (construct (swap [nextpos outpos outval])
                           [[src :>> (swap [inpos nextpos inval])]
                            [aggr inpos inval :> outpos outval]]))))))


;; [pos-syms val-syms] (map #(into (vec %)
;;                                         (v/gen-non-nullable-vars dimensions))
;;                                  (split-at dimensions in-syms))
