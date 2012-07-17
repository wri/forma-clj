(ns forma.ops.classify
  (:use [forma.classify.logistic]
        [cascalog.api])
  (:require [cascalog.ops :as c]
            [forma.thrift :as thrift]))

(defn unpack-feature-vec
  "Creates a persistent vector from forma- and neighbor-val objects;
  building the vector for the logistic classifier."
  [forma-val neighbor-val]
  {:pre [(instance? forma.schema.FormaValue forma-val)
         (instance? forma.schema.NeighborValue neighbor-val)]}
  (let [intercept [1]
        [fire short long t-stat break] (thrift/unpack forma-val)
        fire-seq (thrift/unpack fire)
        [fire-neighbors _ & more] (thrift/unpack neighbor-val) ;; skip count
        fire-neighbor (thrift/unpack fire-neighbors)]
    (into [] (concat intercept fire-seq [short long t-stat break]
                     fire-neighbor more))))

(defn wrap-unpack-feature-vec
  [forma-vec neighbor-obj]
  [(vec (unpack-feature-vec forma-vec neighbor-obj))])

(defbufferop [logistic-beta-wrap [r c m]]
  "Accepts all tuples within an ecoregion and returns a coefficient
  vector resulting from a logistic regression.

  TODO: replace the `maps` with a list comprehension and just
  generally simplify this function.
  See: https://github.com/reddmetrics/forma-clj/issues/105"
  [tuples]
  (let [make-binary  (fn [x] (if (zero? x) 0 1))
        label-seq    (map (comp make-binary first) tuples) 
        val-mat      (map second tuples) 
        neighbor-mat (map last tuples)
        feature-mat  (map unpack-feature-vec val-mat neighbor-mat)]
    (prn (take 10 val-mat))
    [[(logistic-beta-vector
       (to-double-rowmat label-seq)
       (to-double-matrix feature-mat)
       r c m)]]))

(defn logistic-prob-wrap
  [beta-vec val neighbor-val]
  (let [beta-mat (to-double-rowmat beta-vec)
        features-mat (to-double-rowmat (unpack-feature-vec val neighbor-val))]
    (flatten (vec (.toArray (logistic-prob beta-mat features-mat))))))

(defn beta-dict
  "Accepts a source of beta vectors, indexed by ecoregion; returns a
  dictionary with the ecoregions as keys and the beta vectors as
  values."
  [beta-src]
  (let [src (name-vars beta-src ["?s-res" "?eco" "?beta"])
        beta-vec (??<- [?eco ?beta] (src _ ?eco ?beta))]
    (apply merge
           (for [[eco beta] beta-vec]
             {((comp keyword str) eco) beta}))))
