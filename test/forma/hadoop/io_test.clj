(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use midje.sweet
        clojure.test))

(deftest globstring-test
  (are [args result] (= (str "s3n://bucket/" result)
                        (apply globstring "s3n://bucket/" args))
       [* * ["008006" "033011"]] "*/*/{008006,033011}/"))

(deftest count-vals-test
  (are [n thriftable] (= n (count-vals thriftable))
       3 (int-struct [1 3 2])
       1 (int-struct [1])
       3 (int-struct [5.4 32 12.0])))
