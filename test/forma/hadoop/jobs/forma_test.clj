(ns forma.hadoop.jobs.forma-test
  (:use cascalog.api
        [midje sweet cascalog]
        [clojure.string :only (join)] forma.hadoop.jobs.forma)
  (:require [forma.schema :as schema]
            [forma.date-time :as date]
            [forma.trends.filter :as f]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]
            [forma.testing :as t]))

;; Check that each step yields an intermediate result for the local
;; data set, which runs from the end of the preprocessing through the
;; final probablities.

;; We are sourcing from a selection of about 2,300 pixels in
;; Indonesia, of which 1,359 are hit with probabilities above 50% by
;; April 2012.

(def test-map {:est-start "2005-12-31"
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

(fact "Testing that beta-data-prep function yields a specific entry
within the reference data set. "
  (let [ref-src (hfs-seqfile (t/sampledata-path "beta-data-path"))
        ref-tap (<- [?sres ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?eco ?hansen]
                    (ref-src ?sres ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val ?eco ?hansen)
                    (= ?s 429) (= ?l 2204))]
    (beta-data-prep test-map
                    (hfs-seqfile (t/sampledata-path "final-path"))
                    (hfs-seqfile (t/sampledata-path "static-path")))
    => (produces-some (first (??- ref-tap)))))
