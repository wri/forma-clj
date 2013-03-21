(ns forma.hadoop.pail-test
  (:use forma.hadoop.pail
        forma.schema
        midje.sweet)
  (:require [forma.thrift :as thrift])
  (:import [backtype.hadoop.pail Pail]))

;; TODO: Integrate these into some tests.
(def some-pail
  (let [path "/tmp/pail"]
    (try (Pail. path)
         (catch Exception e
           (Pail/create path (pail-structure))))))


;; TODO: Consolidate these two.
(defn gen-tuples [dataset m-res t-res]
  (->> (for [line (range 1000)]
         (thrift/DataChunk* "pixel-chunk"
                            (thrift/ModisPixelLocation* "500" 1 2 3 line)
                            [1 1 1] "16" "2001"))
       (into [])))

(defn tuple-writer [dataset m-res t-res]
  (with-open [stream (.openWrite some-pail)]
    (doseq [data (range 1000)]
      (.writeObject stream
                    (thrift/DataChunk* "pixel-chunk"
                            (thrift/ModisPixelLocation* "500" 1 2 3 4)
                            [1 1 1] "16" "2001")))
    (.consolidate some-pail)))

(future-fact "tests using pedigree field")
