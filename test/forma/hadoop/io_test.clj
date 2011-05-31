(ns forma.hadoop.io-test
  (:use [forma.hadoop.io] :reload)
  (:use midje.sweet
        clojure.test))

(deftest globstring-test
  (are [args result] (= (str "s3n://bucket/" result)
                        (apply globstring "s3n://bucket/" args))
       [* * ["008006" "033011"]] "*/*/{008006,033011}/"))
