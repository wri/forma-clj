(ns forma.hadoop.pail-test
  (:use forma.hadoop.pail
        forma.schema)
  (:import [backtype.hadoop.pail Pail]))

;; TODO: Integrate these into some tests.
(def some-pail
  (let [path "/tmp/pail"]
    (try (Pail. path)
         (catch Exception e
           (Pail/create path (pail-structure))))))

;; TODO: Consolidate these two.
(defn gen-tuples [dataset m-res t-res]
  (->> (for [data (range 1000)]
         (chunk-value dataset t-res "2005-12-01" m-res 8 6 x 1 data))
       (into [])))

(defn tuple-writer [dataset m-res t-res]
  (with-open [stream (.openWrite some-pail)]
    (doseq [data (range 1000)]
      (.writeObject stream
                    (chunk-value dataset t-res "2005-12-01" m-res 8 6 x 1 data)))
    (.consolidate some-pail)))

(future-fact "tuple-writing tests!")

;; (?pail- (split-chunk-tap "/tmp/output")
;;         (<- [?a] ((tuple-src "precl" "1000" "32") ?a)))
