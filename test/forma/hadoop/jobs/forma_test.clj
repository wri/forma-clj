(ns forma.hadoop.jobs.forma-test
  "Check that each step yields an intermediate result for the local
  data set, which runs from the end of the preprocessing through the
  final probablities.

  We are sourcing from a selection of about 2,300 pixels in Indonesia,
  of which 1,359 are hit with probabilities above 50% by April 2012.

  Only certain functions are tested on the small-sample data set,
  since even with the small selection, some of the functions take a
  few minutes to finish -- let alone check certain values. TODO: for
  these functions, come up with a few tests on an _even smaller_
  number of pixels."
  (:use cascalog.api
        [midje sweet cascalog]
        [clojure.string :only (join)] forma.hadoop.jobs.forma)
  (:require [forma.schema :as schema]
            [forma.date-time :as date]
            [forma.trends.filter :as f]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]
            [forma.testing :as t]
            [forma.thrift :as thrift]))

(def test-map
  "Define estimation map for testing based on 500m-16day resolution.
  This is the estimation map that generated the small-sample test
  data."
  {:est-start "2005-12-31"
   :est-end "2012-04-22"
   :s-res "500"
   :t-res "16"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 30
   :window 10
   :ridge-const 1e-8
   :convergence-thresh 1e-6
   :max-iterations 500
   :min-coast-dist 3})

;; Supporting, private functions for the subsequent tests.

(defn- test-path
  [path]
  (hfs-seqfile (t/test-path path)))

(defn- tuple-seq
  "Returns a sequence of tuples from the specified source, or a
  function and associated argument sets that produce a source."
  ([src]
     (first (??- src)))
  ([f & args]
     (let [src (apply f args)]
       (first (??- src)))))

(defn- extract-obj
  "Convenient wrapper for filter function; used mainly to grab a
  thrift object from a cascalog record."
  [cl coll]
  (filter #(instance? cl %) coll))

;; Tests!!!

(facts "Test that the beta-data-prep yields pixel attributes with
  certain characteristics; specifically that the neighbor t-stat
  values of the first and last tuples are equal to known, reference
  values."
  (let [tuples (tuple-seq beta-data-prep
                          test-map
                          (test-path "final-path")
                          (test-path "static-path"))]

  ;; Neighborhood minimimum t-stat value for first tuple
    (apply thrift/get-min-tstat
           (extract-obj forma.schema.NeighborValue (first tuples)))
    => 0.25990809624094535)

  ;; Neighborhood minimimum t-stat value for last tuple
    (apply thrift/get-min-tstat
           (extract-obj forma.schema.NeighborValue (last tuples)))
    => 0.16269026002659434)

(fact "Testing that `forma-estimate` produces the first value of the
  probability sequence is equal to a known, reference value."
  (let [test-src (forma-estimate (test-seq "beta-path")
                                 (test-seq "final-path")
                                 (test-seq "static-path"))
        filtered-src (<- [?s-res ?mod-h ?mod-v ?s ?l ?prob-series]
                         (test-src ?s-res ?mod-h ?mod-v ?s ?l ?prob-series)
                         (= ?s 461) (= ?l 2229))
        tuple (first (tuple-seq filtered-src))]
    (first (last tuple)) => 0.0014698426720482411))

(fact "Testing that `beta-gen` produces the proper coefficient vectors
  for the small-sample data.  There should be two betfa vectors, one
  for each of the two ecoregions represented in the sample data."
  (let [ref-src (hfs-seqfile (t/test-path "beta-path"))
        ref-tap (<- [?s-res ?eco ?beta]
                    (ref-src ?s-res ?eco ?beta))]

    (beta-gen test-map (test-seq "beta-data-path"))
    => (produces (first (??- ref-tap)))))
