(ns forma.ops.classify
  "Convenient wrapper functions for the classify.logistic namespace,
  used mainly to compose cascalog queries."
  (:use [forma.classify.logistic]
        [cascalog.api])
  (:require [cascalog.ops :as c]
            [forma.thrift :as thrift]))

(defn unpack-feature-vec
  "Creates a persistent vector from forma- and neighbor-val objects;
  building the vector for the logistic classifier.  The `[1]`
  indicates the mandatory intercept in the logistic regression."
  [forma-val neighbor-val]
  {:pre [(instance? forma.schema.FormaValueforma-val)
         (instance? forma.schema.NeighborValue neighbor-val)]}
  (let [[fire short long t-stat break] (thrift/unpack forma-val)
        fire-seq (thrift/unpack fire)
        [fire-neighbors _ & more] (thrift/unpack neighbor-val)
        fire-neighbor (thrift/unpack fire-neighbors)]
    (into [] (concat [1] fire-seq [short long t-stat break]
                     fire-neighbor more))))

(defbufferop [logistic-beta-wrap [r c m]]
  "Accepts all tuples within an ecoregion and returns a coefficient
  vector resulting from a logistic regression."
  [tuples]
  (let [make-binary  (fn [x] (if (zero? x) 0 1))
        pixel-features (for [x tuples]
                         (let [[label val neighbor] x]
                           [(make-binary label) (unpack-feature-vec val neighbor)]))]
    [[(logistic-beta-vector (to-double-rowmat (map first pixel-features))
                            (to-double-matrix (map second pixel-features))
                            r c m)]]))

(defn logistic-prob-wrap
  "Accepts the approrpiate coefficient (beta) vector for a given
  pixel, along with that pixel's features (within-pixel and
  neighboring values); returns the probability of deforestation."
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
