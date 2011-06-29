(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use midje.sweet))

(tabular
 (fact "Globstring test."
   (apply globstring "s3n://bucket/" ?args) => (str "s3n://bucket/" ?result))
 ?args                     ?result
 [* * ["008006" "033011"]] "*/*/{008006,033011}/")


;; ## Various schema tests

(def neighbors [(forma-value nil 1 1 1)
                (forma-value nil 2 2 2)])

;.;. The biggest reward for a thing well done is to have done it. --
;.;. Voltaire
(fact
  "Tests that the combine neighbors function produces the proper
textual representation."
  (let [s "1 1 1 1 0 0 0 0 1.0 1.0 1.0 0 0 0 0 2 1.5 1.0 1.5 1.0 1.5 1.0"]
    (textify 1 1 1 1
             (first neighbors)
             (combine-neighbors neighbors)) => s))

(fact
  "Checks that neighbors are being combined properly."
  (let [test-seq [(forma-value nil 1 1 1) (forma-value nil 2 2 2 )]]
    (combine-neighbors test-seq) => (neighbor-value (fire-tuple 0 0 0 0)
                                                    2
                                                    1.5 1.0
                                                    1.5 1.0
                                                    1.5 1.0)))

(tabular
 (fact "count-vals test."
   (count-vals ?thriftable) => ?n
   ?n ?thriftable
   3  (int-struct [1 3 2])
   1  (int-struct [1])
   3  (int-struct [5.4 32 12.0])))
