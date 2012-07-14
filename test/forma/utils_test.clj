(ns forma.utils-test
  (:use forma.utils
        [forma.trends.analysis]
        midje.sweet)
  (:require [forma.testing :as t])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(facts "string conversion tests"  
  (strings->floats "192") => [192.0]
  (strings->floats "192" "12") => [192.0 12.0]
  (strings->floats "192") => (just [float?])
  (strings->floats "192" "12") => (just [float? float?]))

(facts "between? tests."
  (between? -2 2  1) => truthy
  (between? -2 2 -2) => truthy
  (between? -2 2 -3) => falsey)

(facts "Here are some examples of how to thread a value through a
series of functions. This has some advantage over threading with `->`,
as thrush is a function, not a macro, and can evaluate its arguments."
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

(facts "find-first test."
  (find-first pos? [-1 -2 0 3 4 -1 5]) => 3
  (find-first pos? (range -2 4)) => 1)

(facts "scaling test."
  (scale 2 [1 2 3]) => [2 4 6]
  (scale -1 [1 2 3]) => [-1 -2 -3]
  (scale 0 [2 1]) => [0 0])

(facts "weighted-mean tests."
  (weighted-mean 8 3 1 1) => 6.25
  (weighted-mean 8 0 1 1) => 1.0

  "Negative weights aren't acceptable."
  (weighted-mean 8 -1 1 1) => (throws IllegalArgumentException)

  "Values and weights must come in pairs."
  (weighted-mean 8 1 1) => (throws AssertionError))

(fact "positions test."
  (positions pos? [-1 -2 0 6 12 -1 2]) => [3 4 6]
  (positions pos? (range -8 2)) => '(9)
  (positions pos? (range -8 1)) => '())

(fact "trim-seq tests."
  (trim-seq 0 2 1 [1 2 3]) => [1]
  (trim-seq 0 3 0 [1 2 3]) => [1 2 3]
  (trim-seq 5 10 0 [1 2 3]) => [])

(fact "windowed-map test"
  (windowed-map (partial reduce +) 8 (range 10)) => [28 36 44])

(fact "Check average of vector. Casting as float to generalize for
another vector as necessary. Otherwise average will return fractions."
  (float (average [1 2 3.])) => 2.0)

(fact "moving-average test."
  (moving-average 3 (range 10)) => [1 2 3 4 5 6 7 8]
  (moving-average 2 (range 5))  => [1/2 3/2 5/2 7/2])

(fact "Check that indexing is correct"
  (idx [4 5 6]) => [1 2 3]
  (idx (range 3 5)) => [1 2])

;; ## Byte Manipulation Tests

(fact float-bytes => 4)

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

(fact "partition-stream test. These are sort of crappy tests, but they
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
