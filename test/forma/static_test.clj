(ns forma.static-test
  (:use forma.static
        midje.sweet))

(fact "static-datasets field test."
  (for [[_ v] static-datasets]
    (keys v) => (just [:corner :travel :step :nodata] :in-any-order)))
