(ns forma.utils-test
  (:use forma.utils
        midje.sweet)
  (:require [forma.testing :as t])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(facts "between? tests."
  (between? -2 2 1) => truthy
  (between? -2 2 -2) => truthy
  (between? -2 2 -3) => falsey)

(facts "thrush testing!

Here are some examples of how to thread a value through a series of
functions. This has some advantage over threading with `->`, as thrush
is a function, not a macro, and can evaluate its arguments."
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

(facts "Running sum test."
  (running-sum [] 0 + [1 1 1]) => [1 2 3]
  (running-sum [] 0 + [3 2 1]) => [3 5 6])

(facts "weighted-mean tests."
  (weighted-mean 8 3 1 1) => 6.25
  (weighted-mean 8 0 1 1) => 1

  "Negative weights aren't acceptable."
  (weighted-mean 8 -1 1 1) => (throws RuntimeException)

  "Values and weights must come in pairs."
  (weighted-mean 8 1 1) => (throws AssertionError))

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

;; ## Byte Manipulation Tests

(fact float-bytes => 4)

(fact "flipped-endian-float test."
  (flipped-endian-float [0xD0 0x0F 0x49 0x40]) => (float 3.14159)
  (flipped-endian-float [0xD0 0x0F 0x49]) => (throws AssertionError))
