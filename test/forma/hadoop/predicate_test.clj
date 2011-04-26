(ns forma.hadoop.predicate-test
  (:use forma.hadoop.predicate
        cascalog.api
        midje.sweet))

;; Generates combinations of `mod-h`, `mod-v`, `sample` and `line` for
;; use in buffers.
(def pixels
  (memory-source-tap
   (for [mod-h  (range 3)
         sample (range 20)
         line   (range 20)
         :let [val sample, mod-v 1]]
     [mod-h mod-v sample line val])))

(def pixel-tap
  (<- [?mh ?mv ?s ?l ?v]
      (pixels ?mh ?mv ?s ?l ?v)))

