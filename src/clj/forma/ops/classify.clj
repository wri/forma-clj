(ns forma.ops.classify
  (:use [forma.classify.logistic]
        [cascalog.api])
  (:require [cascalog.ops :as c]
            [forma.thrift :as thrift]))

(defn unpack-feature-vec [forma-val neighbor-val]
  "TODO: Convert forma-val to thrift - see forma-seq
  (schema/unpack-forma-val forma-val)"
  (let [intercept [1]
        [fire short long t-stat break] forma-val
        fire-seq (thrift/unpack fire)
        [fire-neighbors _ & more] (thrift/unpack neighbor-val) ;; skip count
        fire-neighbor (thrift/unpack fire-neighbors)]
    (into [] (concat intercept fire-seq [short long t-stat break]
                     fire-neighbor more))))

(defn wrap-unpack-feature-vec
  [forma-vec neighbor-obj]
  [(vec (unpack-feature-vec forma-vec neighbor-obj))])

(defbufferop [logistic-beta-wrap [r c m]]
  "returns a vector of parameter coefficients.  note that this is
  where the intercept is added (to the front of each stacked vector in
  the feature matrix

  TODO: The intercept is included in the feature vector for now, as a
  kludge when we removed the hansen statistic.  When we include the
  hansen stat, we will have to replace the feature-mat binding below
  with a line that tacks on a 1 to each feature vector.
  "
  [tuples]
  (let [make-binary  (fn [x] (if (zero? x) 0 1))
        label-seq    (map (comp make-binary first) tuples) 
        val-mat      (map second tuples) 
        neighbor-mat (map last tuples)
        feature-mat  (map unpack-feature-vec val-mat neighbor-mat)]
    [[(logistic-beta-vector
       (to-double-rowmat label-seq)
       (to-double-matrix feature-mat)
       r c m)]]))

(defn logistic-prob-wrap
  [beta-vec val neighbor-val]
  (let [beta-mat (to-double-rowmat beta-vec)
        features-mat (to-double-rowmat (unpack-feature-vec val neighbor-val))]
    (flatten (vec (.toArray (logistic-prob beta-mat features-mat))))))

(defbufferop mk-timeseries
  [tuples]
  [[(map second (sort-by first tuples))]])

(defn mk-key
  [k]
  (keyword (str k))) 

(defn make-dict
  [v]
  {(mk-key (second v))
   (last v)})

(defn beta-dict
  "create dictionary of beta vectors"
  [beta-src]
  (let [src (name-vars beta-src ["?s-res" "?eco" "?beta"])
        beta-vec (first (??- (c/first-n src 25000)))]
    (apply merge-with identity
           (map make-dict beta-vec))))
