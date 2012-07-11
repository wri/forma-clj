(ns forma.ops.classify-test
  "This namespace tests the wrappers around the logistic classifier
  functions that generate the coefficient vectors based on the
  training data."
  (:use forma.ops.classify :reload)
  (:use cascalog.api)
  (:require [forma.testing :as t]))

;; (def est-map {:convergence-thresh 1.0E-6,
;;               :ridge-const 1.0E-8,
;;               :neighbors 1,
;;               :est-end "2012-04-22",
;;               :window-dims [600 600],
;;               :s-res "500",
;;               :t-res "16",
;;               :vcf-limit 25,
;;               :est-start "2005-12-31",
;;               :min-coast-dist 3,
;;               :max-iterations 500,
;;               :long-block 30,
;;               :window 10})

;; (defn my-test []
;;   (let [src (hfs-seqfile (t/dev-path "/testdata/estimation-seqfile"))]
;;     ))
