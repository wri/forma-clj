(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use midje.sweet
        clojure.test))

(deftest globstring-test
  (are [args result] (= (str "s3n://bucket/" result)
                        (apply globstring "s3n://bucket/" args))
       [* * ["008006" "033011"]] "*/*/{008006,033011}/"))

(deftest num-ints-test
  (are [xs] (= (num-ints (int-struct xs))
               (count xs))
       [1 3 2]
       [1]
       [5.4 32 12.0]))
