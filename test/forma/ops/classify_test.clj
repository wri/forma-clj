(ns forma.ops.classify-test
  "This namespace tests the wrappers around the logistic classifier
  functions that generate the coefficient vectors based on the
  training data."
  (:use forma.ops.classify :reload)
  (:use cascalog.api
        [midje sweet cascalog])
  (:require [forma.testing :as t]
            [forma.date-time :as date]))

(def test-map {:convergence-thresh 1.0E-6
               :ridge-const 1.0E-8
               :t-res "16"
               :est-start "2005-12-31"
               :max-iterations 500})

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
        [sres eco beta] (ffirst (??- (generate-betas test-map src)))]
    eco => 40157
    (first beta) => (roughly -8.206515)
    (last beta)  => (roughly 1.1485239)))

