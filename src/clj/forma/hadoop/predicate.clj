(ns forma.hadoop.predicate
  (:use cascalog.api
        [forma.matrix.utils :only (sparse-expander matrix-of)]
        [forma.reproject :only (pixels-at-res)])
  (:require [forma.utils :as u]
            [forma.schema :as schema]
            [forma.hadoop.io :as io]
            [clojure.string :as s]
            [cascalog.ops :as c]
            [cascalog.vars :as v]
            [cascalog.conf :as conf]
            [hadoop-util.core :as hadoop])
  (:import [org.apache.hadoop.mapred JobConf]
           [cascading.flow.hadoop HadoopFlowProcess]
           [cascading.tuple TupleEntryCollector]
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

(defn liberate
  "Takes a line with an index as the first value and numbers as the
  rest, and converts it into a 2-tuple formatted as `[idx, row-vals]`,
  where `row-vals` are sealed inside a clojure vector.

  Example usage:

    (liberate \"1 12 13 14 15\")
    ;=> [1 [12 13 14 15]]"
  [line]
  (let [[idx & row-vals] (map u/read-numbers
                              (s/split line #"\s+"))]
    [idx (vec row-vals)]))

(defmapop flatten-window
  "Flattens a window of nested clojure vectors into a single
  vector. For example:

    (flatten-window ?chunk :> ?vector)"
  [window]
  [(into [] (flatten window))])

;; #### Defmapcatops

(defmapcatop index
  "splits a sequence of values into nested 2-tuples formatted as
  `<idx, val>`. Index supports the `zero-index` keyword argument; this
  defaults to zero."
  [xs & {:keys [zero-index]
         :or {zero-index 0}}]
  (map-indexed (fn [idx val]
                 [(+ idx zero-index) val])
               xs))

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
  [val] #(>= % limit))

(defpredsummer [bi-filtered-count [lim1 lim2]]
  [val1 val2] #(and (>= %1 lim1)
                    (>= %2 lim2)))

(defpredsummer full-count
  [val] identity)

;; ### Predicate Macros

;; TODO: Convert to dynamically opening business with predmacro.

(def converter
  "Converts between a textline with two numbers encoded as strings and
   their integer representations."
  (<- [?textline :> ?country ?admin]
      (mangle #"," ?textline :> ?country ?admin-s)
      (u/strings->floats ?admin-s :> ?admin)))

(def blossom-chunk
  "Returns a Cascalog predicate macro that emits MODIS pixel values for each
  element in a DataChunk array.

  A DataChunk Thrift object is composed with a DataValue Thrift object which is
  an array of pixel values representing values at different periods. This query
  emits a tuple for each element in that array.

  Input variables:
    ?chunk - A DataChunk Thrift object.

  Output variables:
    ?s-res - The spatial resolution in meters as a string.
    ?mod-h - The MODIS tile coordinate column.
    ?mod-v - The MODIS tile coordinate row.
    ?sample - The MODIS tile pixel row.
    ?line - The MODIS tile pixel column.
    ?val - The pixel value

  Example usage:

    (let [src [[dc]]] ;; dc is a DataChunk Thrift object
         (??<-
           [?s-res ?mod-h ?mod-v ?sample ?line ?val]
           (src ?chunk)
           (blossom-chunk ?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?val)))"
  (<- [?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (index ?static-chunk :> ?pix-idx ?val)      
      (schema/chunk-locval ?chunk :> ?loc ?static-chunk)
      (schema/chunkloc->pixloc ?loc ?pix-idx :> ?pixloc)
      (schema/pixel-prop-location ?pixloc :> ?pixlocval)
      (schema/unpack-pixel-location ?pixlocval :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn chunkify [chunk-size]
  (<- [?dataset !date ?s-res ?t-res ?mh ?mv ?chunkid ?chunk :> ?datachunk]
      (schema/chunk-location ?s-res ?mh ?mv ?chunkid chunk-size :> ?location)
      (schema/mk-data-value ?chunk :> ?data-val)
      (schema/chunk-value ?dataset ?t-res !date ?location ?data-val :> ?datachunk)))

(def break
  "Takes a source of textlines representing rows of a gridded
  dataset (with indices prepended onto each row), and generates a
  source of `row`, `col` and `val`."
  (<- [?line :> ?row ?col ?val]
      (liberate ?line :> ?row ?row-struct)
      (index ?row-struct :> ?col ?val)))

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

(defn pixel-generator
  "Returns a Cascalog generator that emits MODIS pixel tuples
  [?mod-h ?mod-v ?sample ?line] for a set of MODIS tiles at a given spatial
  resolution.

  Arguments:
    tmp-path - A staging directory for writing tuples to a sequence file.
    res - The spatial resolution.
    tileseq - Map of country ISO keywords to MODIS tiles (see: forma.source.tilesets)"
  [tmp-path res tileseq]
  {:pre [(coll? tileseq) 
         (string? res)]}
  (let [tap (:sink (hfs-seqfile tmp-path))]
    (with-open [^TupleEntryCollector collector
                (-> (hadoop/job-conf (conf/project-conf))
                    (HadoopFlowProcess.)
                    (.openTapForWrite tap))]
      (doseq [item (for [[h v]  tileseq
                         sample (range (pixels-at-res res))
                         line (range (pixels-at-res res))]
                     [h v sample line])]
        (.add collector (Util/coerceToTuple item))))
    (name-vars tap ["?mod-h" "?mod-v" "?sample" "?line"])))

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
        dim-vec         (if (coll? dim-vec) dim-vec [dim-vec])
        get-length     #(try (dim-vec %) (catch Exception e (last dim-vec)))]
    (reduce #(%2 %1)
            gen
            (for [[dim inpos] (map-indexed vector in-syms)
                  :let [empty-val (->> (get-length (max 0 (dec dim)))
                                       (matrix-of sparse-val dim))
                        aggr (vals->sparsevec (get-length dim) empty-val)]]
              (fn [src]
                (construct (swap-syms gen [inpos val] [outpos outval])
                           [[src :>> (get-out-fields gen)]
                            [aggr inpos val :> outpos outval]]))))))
