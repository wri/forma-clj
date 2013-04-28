(ns forma.source.gadmiso-test
  (:use [forma.source.gadmiso] :reload)
  (:use [midje sweet cascalog])
  (:require [forma.utils :as utils :only (ls)]))

(fact "Test `text-map`."
  (first text-map) => "AFG,1"
  (last text-map) => "UNK,-9999"
  (count text-map) => 39163)

(fact "Test `parse-line`."
  (parse-line "AFG,1") => {1 "AFG"}
  (parse-line "AFG") => (throws NullPointerException))

(fact "Test that `gadm-iso-map` is constructed correctly, works as expected."
  (gadm-iso-map 1) => "AFG"
  (gadm-iso-map -9999) => "UNK")

(fact "Test that `gadm->iso` correctly wraps `gadm-iso-map`."
  (gadm->iso 1) => "AFG"
  (gadm->iso -9999) => "UNK")

(fact "Check decompression using `zip->decomp-dir`."
  (let [uuid "12345"]
    (zip->decomp-dir uuid) => (format "/tmp/%s" uuid)
    (-> (format "/tmp/%s" uuid)
        (utils/ls)
        (first)
        (.getPath) => (format "/tmp/gadm2/%s/gadm2.csv" uuid))))

(fact "Check `csv-dir->line-seq`."
  (let [uuid "12345"
        path (zip->decomp-dir uuid)
        lines (csv-dir->line-seq path)]
    (first lines) => "iso,objectid,id_0,id_1,id_2,id_3,id_4,id_5"
    (second lines) => "AFG,1,1,12,129,0,0,0"))

(fact "Check `mk-id-kws`."
  (let [coll (range 1 5)] ;; keywords are created based on count of coll
    (mk-id-kws coll)) => [:id0 :id1 :id2 :id3])

(fact "Check `fields->object-id"
  (fields->object-id ["IDN" 1 2 3 4 5 6 7]) => 1
  (fields->object-id [4 5 6]) => 5)

(fact "Check `fields->iso`."
  (fields->iso ["IDN" 1 2 3 4 5 6 7]) => "IDN"
  (fields->iso [1 "IDN" 2 3 4 5 6 7]) => (throws AssertionError))

(fact "Check `fields->ids`."
  (fields->ids ["IDN" 1 2 3 4 5 6 7]) => (range 2 (inc 7)))

(fact "Check `mk-ids-val`."
  (mk-ids-val ["IDN" 1 2 3 4 5 6 7]) => {:id0 2 :id1 3 :id2 4 :id3 5 :id4 6 :id5 7})

(fact "Check `line->fields`."
  (line->fields "IDN,1,2,3,4,5,6,7") => ["IDN" 1 2 3 4 5 6 7]
  (line->fields "0,1,2,3,4,5,6,7") => ["0" 1 2 3 4 5 6 7])

(fact "Check `line->gadm2-map.`"
  (line->gadm2-map "IDN,1,2,3,4,5,6,7")
  => {1 {:iso "IDN" :ids {:id5 7, :id4 6, :id3 5, :id2 4, :id1 3, :id0 2}}})

(fact "Check `gadm2->map`."
  (gadm2-map 1) => {:iso "AFG", :ids {:id5 0, :id4 0, :id3 0, :id2 129, :id1 12, :id0 1}})

(fact "Test that `gadm2->iso` works."
  (gadm2->iso 1) => "AFG"
  (gadm2->iso 100000) => "KEN"
  (gadm2->iso -9999) => "UNK")
