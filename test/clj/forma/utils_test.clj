(ns forma.utils-test
  (:use forma.utils :reload)
  (:use forma.trends.analysis
        [midje sweet cascalog]
        cascalog.api)
  (:require [forma.testing :as t]
            [forma.thrift :as thrift])
  (:require [forma.testing :as t])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(facts "string conversion tests."  
  ;; Floats
  (strings->floats "192") => [192.0]
  (strings->floats "192" "12") => [192.0 12.0]
  
  ;; Type checking
  (strings->floats "192") => (just [float?])
  (strings->floats "192" "12") => (just [float? float?]))

(facts "between? tests."
  (between? -2 2 1)  => truthy
  (between? -2 2 -2) => truthy
  (between? -2 2 -3) => falsey)

(facts "thrush testing examples of how to thread a value through a
  series of functions. This has some advantage over threading with
  `->`, as thrush is a function, not a macro, and can evaluate its
  arguments."
  (thrush 2 #(* % 5) (partial + 2)) => 12
  (apply thrush 1 (for [num (range 4)]
                    (fn [x] (+ x num)))) => 7)

(fact "nth-in test."
  (nth-in [[1 2] [3 4]] [0]) => [1 2]
  (nth-in [[1 2] [3 4]] [0 1]) => 2)

(facts "unweave test."
  (unweave [0 1 2 3]) => [[0 2] [1 3]]
  (unweave [1 2 3]) => (throws AssertionError)
  (unweave []) => (throws AssertionError))

(facts "scaling test."
  (scale 2 [1 2 3]) => [2 4 6]
  (scale -1 [1 2 3]) => [-1 -2 -3]
  (scale 0 [2 1]) => [0 0])

(facts "weighted-mean tests."
  (weighted-mean 8 3 1 1) => 6.25
  (weighted-mean 8 0 1 1) => 1.0

  ;; Negative weights aren't acceptable.
  (weighted-mean 8 -1 1 1) => (throws AssertionError)

  ;; Values and weights must come in pairs.
  (weighted-mean 8 1 1) => (throws AssertionError))

(fact "trim-seq tests."
  (trim-seq 0 2 1 [1 2 3]) => [1]
  (trim-seq 0 3 0 [1 2 3]) => [1 2 3]
  (trim-seq 5 10 0 [1 2 3]) => [])

;; ## IO Tests

(def test-gzip (t/dev-path "/testdata/sample.txt.gz"))
(def test-txt  (t/dev-path "/testdata/sample.txt"))

(facts "forma-flavored `input-stream` testing."
  (with-open [gzip (input-stream test-gzip)
              txt  (input-stream test-txt)]
    (type gzip) => #(isa? % GZIPInputStream)
    (type txt)  => #(isa? % InputStream)))

(facts "force-fill test."
  (with-open [txt (input-stream test-txt)]
    (let [arr (byte-array 10)
          bytes-filled (force-fill! arr txt)]
      bytes-filled => 10
      (every? (complement zero?) arr) => truthy)))

(fact "Partition-stream test. These are sort of crappy tests, but they
  do show that we have a sequence of byte arrays being generated."
  (with-open [txt (input-stream test-txt)]
    (let [result (partition-stream 10 txt)]
      (count (first result)) => 10
      (type (first result)) => byte-array-type
      (count result) => 6)))

(tabular
 (fact "read numbers testing."
   (read-numbers ?input) => ?output)
 ?input ?output
 "100"  100
 "-2.9" -2.9
 "face" (throws AssertionError))

;; ## Byte Manipulation Tests

(fact float-bytes => 4)

(fact
  "Check average of vector. Casting as float to generalize for another
  vector as necessary. Otherwise average will return fractions."
  (float (average [1 2 3.])) => 2.0)

(fact
  "Check that indexing is correct"
  (idx [4 5 6]) => [1 2 3])

(tabular
 (fact
  "check scaling all elements of a vector by a scalar"
  (scale ?scalar ?coll) => ?expected)
 ?scalar ?coll ?expected
 1 [1 2 3] [1 2 3]
 2 [1 2 3] [2 4 6]
 1.5 [1 2 3] [1.5 3.0 4.5])

(fact "Check windowed-map"
  (windowed-map average 2 [1 2 50]) => [3/2 26])

(tabular
 "Check get-replace-vals-locs"
 (fact
   (get-replace-vals-locs -9999 ?coll -1) => ?result)
 ?coll                           ?result
 [1 -9999 -9999 2 3 4 -9999 5]  {1 1, 2 1, 6 4}
 [-9999 -9999 -9999 2 3 4 -9999 5]  {0 -1, 1 -1, 2 -1, 6 4}
 [-9999 -9999.0 -9999 2 3 4 -9999 5] {0 -1, 2 -9999.0, 6 4})

(facts
  "Check replace-from-left"
  (replace-from-left -9999 [1 2 -9999 3 -9999 5]) => [1 2 2 3 3 5]
  (replace-from-left -9999 [-9999 2 -9999 3 -9999 5]) => [nil 2 2 3 3 5]
  (replace-from-left -9999 [-9999 2 -9999 3 -9999 5] :default -1)
   => [-1 2 2 3 3 5]
   (replace-from-left -9999 [-9999 -9999 -9999 3 -9999 5]) => [nil nil nil  3 3 5]
   (replace-from-left -9999 [1 -9999 -9999.0 3]) => [1 1 -9999.0 3]
   (replace-from-left -9999 [1 -9999.0 -9999 3]) => [1 -9999.0 -9999.0 3])

(fact
  "Check nested replace-from-left*"
  (replace-from-left* -9999 [1 2 -9999 3 -9999 5]) => [[1 2 2 3 3 5]])

(facts
  "Check `objs-contains-nodata?"
  (obj-contains-nodata? -9999. (thrift/FormaValue*
                                  (thrift/FireValue* 1 1 1 1)
                                  -9999. 1. 1. 1.)) => true
  (obj-contains-nodata? -9999. (thrift/FormaValue*
                                  (thrift/FireValue* 1 1 1 1)
                                  1. 1. 1. 1.)) => false)

(fact
  "Check `filter*`"
  (let [src [[1 [2 3 2]] [3 [5 nil 6]]]]
       (<- [?a ?all-twos]
           (src ?a ?b)
           (filter* (partial = 2) ?b :> ?all-twos))) => (produces [[1 [2 2]] [3 []]]))

(fact
  "Check `replace-all`"
  (replace-all nil -9999 [1 nil 3]) => [1 -9999 3]
  (replace-all -9999 nil [1 -9999 3]) => [1 nil 3]
  (replace-all -9999.0 nil [1 -9999 3]) => [1 -9999 3])

(fact
  "Check `replace-all*`"
  (let [to-replace 2
        replacement nil
        src [[[1 2 3]]]]
    (<- [?new-series]
        (src ?series)
        (replace-all* to-replace replacement ?series :> ?new-series))) => (produces [[[1 nil 3]]]))
