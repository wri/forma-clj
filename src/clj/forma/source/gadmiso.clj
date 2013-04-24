(ns forma.source.gadmiso
  (:use [forma.utils :as utils :only (unzip)])
  (:require [clojure.java.io :as io]))

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

(defn unzip-gadm2
  [path & [output-dir]]
  (let [csv-name "gadm2.csv"
        output-dir (or output-dir "/tmp")]
    (do
      (-> (.getPath path)
          (utils/unzip output-dir)))
    (str output-dir "/" csv-name)))

(defn zip->line-seq
  [zip-name]
  (-> zip-name
      io/resource
      unzip-gadm2
      io/reader
      line-seq))

(defn mk-id-kws
  [coll]
  (let [fmt-str "id%s"]
    (->> (count coll)
         (range)
         (map (partial format fmt-str))
         (map keyword))))

(defn parse-gadm2-line
  [line]
  (let [fields (.split line ",")
        object-id (read-string (second fields))
        iso (first fields)
        ids (map read-string (drop 2 fields))
        id-kws (mk-id-kws ids)]
    {object-id
     {:iso iso
      :ids (zipmap id-kws ids)}}))

(def gadm2-map
  (->> (zip->line-seq "gadm2.zip")
       (map parse-gadm2-line)
       (apply merge)))

(defn gadm2->iso
  [object-id]
  (:iso (gadm2-map object-id)))

