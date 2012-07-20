(ns forma.source.gadmiso
  (:require [clojure.java.io :as io]))

(def text-map
  "Returns a sequence of vectorized strings (e.g., [\"AFG,1\"])
  from the admin-map located in the resources folder."
  (-> "admin-map.csv" io/resource io/reader line-seq))

(defn parse-line
  "Accepts a vectorized string, as outputted by `text-map`, and
  returns a single hash-map with the GADM ID (integer) as the key and
  the ISO3 code (string) as the value."
  [x]
  (let [[iso gadm-str] (.split x ",")]
    {(read-string gadm-str) iso}))

(def gadm-iso
  "Returns a single hash-map that associates all GADMs and ISO codes."
  (let [small-maps (map parse-line text-map)]
    (apply merge small-maps)))

(defn gadm->iso
  "Accepts an integer GADM ID and returns the associated ISO3 code as
  a string; wraps gadm-iso map for use in a cascalog query"
  [gadm]
  (gadm-iso gadm))
