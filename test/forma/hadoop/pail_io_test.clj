(ns forma.hadoop.pail-io-test
  (:use forma.hadoop.pail-io
        midje.sweet
        midje.cascalog)
  (:import [backtype.hadoop.pail Pail]
           [forma.hadoop.pail SplitDataChunkPailStructure]))

(def some-pail
  (let [path "/tmp/pail"]
    (try (Pail. path)
         (catch Exception e
           (Pail/create path (SplitDataChunkPailStructure.))))))

(defn gen-tuples [dataset m-res t-res]
  (into []
        (for [x (range 1000)]
          (let [data (mk-data-value x :int)]
            (mk-chunk dataset t-res "2005-12-01" m-res 8 6 x 1 data)))))

(defn tuple-writer [dataset m-res t-res]
  (with-open [stream (.openWrite some-pail)]
    (doseq [x (range 1000)]
      (let [data (mk-data-value x :int)]
        (.writeObject stream (mk-chunk dataset t-res "2005-12-01" m-res 8 6 x 1 data))))
    (.consolidate some-pail)))

(future-fact "tuple-writing tests!")

;; (?pail- (split-chunk-tap "/tmp/output")
;;         (<- [?a] ((tuple-src "precl" "1000" "32") ?a)))
