(ns forma.hadoop.jobs.scatter-test
  (:use cascalog.api
        forma.hadoop.jobs.scatter        
        [midje sweet cascalog])
  (:require [forma.thrift :as thrift]
            [forma.hadoop.predicate :as p]
            [forma.reproject :as r]
            [cascalog.ops :as c]
            [forma.hadoop.jobs.forma :as forma])
  (:require [forma.thrift :as thrift])
  (:import [backtype.hadoop.pail Pail]))

;; Rain Testing

(def test-gadm
  [["1000" 1 1 1 1 5]
   ["1000" 1 1 1 2 5]
   ["1000" 1 1 1 3 5]
   ["1000" 1 1 1 4 5]
   ["1000" 1 1 1 5 5]
   ["1000" 1 1 1 7 6]
   ["1000" 1 1 1 8 6]
   ["1000" 1 1 1 9 6]])

(def test-rain
  [[1 1 1 1 "2005-12-01" 12.0]
   [1 1 1 2 "2005-12-01" 2.0]
   [1 1 1 3 "2005-12-01" 13.0]
   [1 1 1 4 "2005-12-01" 14.0]
   [1 1 1 5 "2005-12-01" 15.0]
   [1 1 1 2 "2006-01-01" 15.0]
   [1 1 1 2 "2006-02-01" 16.0]
   [1 1 1 2 "2006-03-01" 16.0]
   [1 1 1 8 "2006-03-01" 16.0]])

(let [results [[5 "2005-12-01" 11.2]
               [5 "2006-01-01" 15.0]
               [5 "2006-02-01" 16.0]
               [5 "2006-03-01" 16.0]
               [6 "2006-03-01" 16.0]]]
  (future-fact?- "This now fails, since static-tap is gone. fix!"
                 results
                 (run-rain ..gadm-src.. ..rain-src..)
                 (provided
                   (static-tap ..gadm-src..) => test-gadm
                   (rain-tap ..rain-src..) => test-rain)))


;; Trying to figure out what's up with rain.

(defn blossomer
  [chunk-src]
  (<- [?s-res ?h ?v ?sample ?line ?val]
      (chunk-src ?tile-chunk)
      (thrift/unpack ?tile-chunk :> _ ?tile-loc ?data _ _)
      (thrift/unpack* ?data :> ?data-value)
      (p/index ?data-value :> ?pixel-idx ?val)
      (thrift/unpack ?tile-loc :> ?s-res ?h ?v ?id ?size)
      (r/tile-position ?s-res ?size ?id ?pixel-idx :> ?sample ?line)))
      
(comment
  (def blossomed-playground
  (??- (p/blossom-chunk tile-chunk-tap))))


;; get a single real DataChunk object - this is VCF
;; wrap it in vector to make it a tap
(def vcf-chunk-tap
  (let [dc (first (take 2 (Pail. "/Users/robin/Downloads/pail/")))]
    [["" dc]]))

;; blossomer returns the expected tap for 24000 pixels
(comment
  (def blossomed (??- (p/blossom-chunk vcf-chunk-tap))))

;; now need to create a DataChunk object for rain

(def vcf-val (second (flatten vcf-chunk-tap)))
(def vcf-val* (thrift/unpack vcf-val))

(def custom-vcf
  (let [[s-res h v id size] ["500" 13 8 3 24000]
        loc (thrift/ModisChunkLocation* s-res (long h) (long v) id size)
        val (thrift/mk-array-value (thrift/pack (repeat size 50)))]
    (thrift/DataChunk* "vcf" loc val "00")))

(def custom-vcf* (thrift/unpack vcf-val))

(def rain-val
  (let [[s-res h v id size] (thrift/unpack (second custom-vcf*))
        ts (thrift/TimeSeries* 0 2 [1 2 3])        
        loc (thrift/ModisPixelLocation* "500" (long h) (long v) 0 0)]
    (thrift/DataChunk* "precl" loc ts s-res)))


;; this should match up with first value out of pail
(def rain-val2
  (let [[s-res h v id size] (thrift/unpack (second custom-vcf*))
        ts (thrift/TimeSeries* 0 2 [1 2 3])        
        loc (thrift/ModisPixelLocation* "500" (long h) (long v) 0 30)]
    (thrift/DataChunk* "precl" loc ts s-res)))

(def rain-tap
  [["" rain-val]])

(defn mk-vcf-src
  [vcf-num]
  (let [vals (take vcf-num (Pail. "/Users/robin/Downloads/pail/"))]
    (partition 2 (filter (complement nil?) (interpose 1 (concat [nil] vals))))))

(fact
  "Checks filter-query, pulling two VCF values out of pail and filtering on min"
  (let [vcf-num 2
        vcf (mk-vcf-src vcf-num)
        rain [[1 rain-val2]]
        min 0]
    (forma/filter-query vcf min rain)) => (produces [["500" 13 8 0 30 0 [1 2 3]]]))