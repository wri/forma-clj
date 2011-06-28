(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use midje.sweet))

(tabular
 (fact "Globstring test."
   (apply globstring "s3n://bucket/" ?args) => (str "s3n://bucket/" ?result))
 ?args                     ?result
 [* * ["008006" "033011"]] "*/*/{008006,033011}/")

(tabular
 (fact "count-vals test."
   (count-vals ?thriftable) => ?n
   ?n ?thriftable
   3  (int-struct [1 3 2])
   1  (int-struct [1])
   3  (int-struct [5.4 32 12.0])))
