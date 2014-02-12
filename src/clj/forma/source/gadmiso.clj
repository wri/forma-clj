(ns forma.source.gadmiso
  (:require [clojure.java.io :as io]
            [forma.utils :as utils :only (unzip ls)]))

(def text-map
  "Returns a sequence of vectorized strings (e.g., [\"AFG,1\"])
  from the admin-map located in the resources folder."
  (-> "admin-map.csv" io/resource io/reader line-seq))

(defn parse-line
  "Accepts a vectorized string, as outputted by `text-map`, and
  returns a single hash-map with the GADM ID (integer) as the key and
  the ISO3 code (string) as the value.

  On the off chance there is more than two fields (e.g. [\"AFG,1,1\"]),
  only the first two will be extracted."
  [x]
  (let [[iso gadm-str] (.split x ",")]
    {(read-string gadm-str) iso}))

(def gadm-iso-map
  "Hash-map that associates all GADMs and ISO codes."
  (let [small-maps (map parse-line text-map)]
    (apply merge small-maps)))

(defn gadm->iso
  "Accepts an integer GADM ID and returns the associated ISO3 code as
  a string; wraps gadm-iso map for use in a cascalog query"
  [gadm]
  (gadm-iso-map gadm))

(defn zip->decomp-dir
  "Decompress gadm2.zip and return the output directory."
  [& [uuid]]
  (let [zip-name "gadm2.zip"
        base-dir "/tmp/gadm2"
        path (.getPath (io/resource zip-name))
        uuid (or uuid (utils/gen-uuid))
        decomp-dir (str base-dir "/" uuid)]
    (do (utils/unzip path decomp-dir))
    decomp-dir))

(defn csv-dir->line-seq
  "Read csv file in `dir` and return a lazy line-seq."
  [dir]
  (-> (utils/ls dir)
      (first)
      (.getPath)
      (io/reader)
      (line-seq)))

(defn mk-id-kws
  "Returns a list of kewords of form `id<n>` based on number `n` of
   items in `coll`.

   Usage:
     (mk-id-kws [1 2 3 4]) => (:id0 :id1 :id2 :id3)
     (mk-id-kws [10 20 30 40]) => (:id0 :id1 :id2 :id3)"
  [coll]
  (let [fmt-str "id%s"]
    (->> (count coll)
         (range)
         (map (partial format fmt-str))
         (map keyword))))

(defn fields->object-id
  "Extract objectid from `fields` coll. Objectid is the second element
   in the collection.

   Usage:
     (fields->object-id [\"IDN\" 1 2 3 4 5 6 7]) => 1"
  [fields]
  (second fields))

(defn fields->iso
  "Extract the iso country code from `fields` coll. ISO code is the first
   element in the collection. ISO code must be a string."
  [fields]
  {:post [(string? %)]}
  (first fields))

(defn fields->ids
  "Given a collection of fields, returns all but the first two
  elements. The first two elements are ISO code and objectid, and the
  rest are id fields."
  [fields]
  (drop 2 fields))

(defn mk-ids-val
  "Given a collection of field values, extracts the id fields (all but first 2 fields),
   and returns a map of id field name to value.

   Usage:
     (mk-ids-val [\"IDN\" 1 2 3 4 5 6 7]) => {:id0 2 :id1 3 :id2 4 :id3 5 :id4 6} :id5 7"
  [fields]
  (-> (fields->ids fields)
      (mk-id-kws)
      (zipmap (fields->ids fields))))

(defn line->fields
  "Parse incoming line, splitting on commas and de-stringing fields as appropriate."
  [line]
  (let [elems (.split line ",")]
    (into [(first elems)] (map read-string (rest elems)))))

(defn line->gadm2-map
  "Given a line from gadm2.csv, return a map where the key is object
   id and the value is another map containing the iso code and the
   various id fields.

   Usage:
     (line->gadm2-map \"IDN,1,2,3,4,5,6,7\")
     ;=> {1 {:iso \"IDN\", :ids {:id5 7, :id4 6, :id3 5, :id2 4, :id1 3, :id0 2}}}"
  [line]
  (let [fields (line->fields line)]
    {(fields->object-id fields)
     {:iso (fields->iso fields)
      :ids (mk-ids-val fields)}}))

(def neg9999
  {:iso "UNK"
   :ids {:id0 -9999, :id1 -9999, :id2 -9999, :id3 -9999, :id4 -9999, :id5 -9999}})

(def gadm2-map
  (->> (zip->decomp-dir)
       (csv-dir->line-seq)
       (rest)
       (map line->gadm2-map)
       (apply merge)
       (#(assoc % -9999 neg9999))))

(defn gadm2->iso
  [object-id]
  (:iso (gadm2-map object-id)))
