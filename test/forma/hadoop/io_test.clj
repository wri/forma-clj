(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use midje.sweet))

(tabular
 (fact "Globstring test."
   (apply globstring ?args) => ?result)
 ?args                        ?result
 [* "hi" ["008006" "033011"]] "*/hi/{008006,033011}")
