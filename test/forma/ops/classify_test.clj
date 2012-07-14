(ns forma.ops.classify-test
  "This namespace tests the wrappers around the logistic classifier
  functions that generate the coefficient vectors based on the
  training data."
  (:use forma.ops.classify :reload)
  (:use cascalog.api
        [midje sweet cascalog])
  (:require [forma.testing :as t]
            [forma.date-time :as date]
            [forma.thrift :as thrift]))

(def test-map {:convergence-thresh 1.0E-6
               :ridge-const 1.0E-8
               :t-res "16"
               :est-start "2005-12-31"
               :max-iterations 500})

(defn- count-internal [x]
  (if-not (instance? clojure.lang.PersistentVector x)
    (count (thrift/unpack x))
    (count x)))

(fact "Test the creation of a feature vector from a FormaValue and
NeighborValue. TODO: rewrite tests when issue #90 is resolved. "
  (let [fire-val (thrift/FireValue* 1 1 1 1)
        forma-val [fire-val 3.0 3.0 3.0 3.0]
        neighbor-val (thrift/NeighborValue* fire-val 1 2.0 3.0 4.0 5.0 6.0 7.0)
        unpacked (unpack-feature-vec forma-val neighbor-val)]
    unpacked => [1 1 1 1 1 3.0 3.0 3.0 3.0 1 1 1 1 2.0 3.0 4.0 5.0 6.0 7.0 0.0 0.0]))

(defn- generate-betas
  [{:keys [convergence-thresh t-res ridge-const est-start max-iterations]} src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?s-res ?eco ?beta]
        (src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?eco ?hansen)
        (logistic-beta-wrap [ridge-const convergence-thresh max-iterations]
                                ?hansen ?val ?neighbor-val :> ?beta)
        (:distinct false))))

(fact "Check the structural (and numerical) stability of the output
from the beta generation queries."
  (let [src (hfs-seqfile (t/dev-path "/testdata/estimation-seqfile"))
        [sres eco beta] (ffirst (??- (generate-betas test-map src)))
        fire-val (thrift/FireValue* 1 1 1 1)
        forma-val [fire-val 3.0 3.0 3.0 3.0]
        neighbor-val (thrift/NeighborValue* fire-val 1 2.0 3.0 4.0 5.0 6.0 7.0)]
    eco => 40157
    (first beta) => (roughly -8.206515)
    (last beta)  => (roughly 1.1485239)
    (logistic-prob-wrap beta forma-val neighbor-val) => [2.88252532638858E-5]))

(fact
  (let [src (hfs-seqfile (t/dev-path "/testdata/estimation-seqfile"))
        beta-src (generate-betas test-map src)]
    (set (keys (beta-dict beta-src))) => (set [:40160 :40157])))
