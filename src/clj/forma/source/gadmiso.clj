(ns forma.source.gadmiso
  (:use cascalog.api)
  (:require [clojure.string :as string]
            [clojure-csv.core :as csv]))

(def text-in
  "Reads `admin-map.csv` into a Clojure data structure, specifically a
  lazy sequence of vectors of the form [\"AFG\" \"1\"].  Note that the
  last line is empty, necessitating the use of `butlast`"
  (butlast (csv/parse-csv (slurp "resources/admin-map.csv"))))

(defn parse-line
  "Accepts a vector of two strings of the form [\"AFG\" \"1\"],
  reflecting each element of `text-in`.  Returns a single hash-map
  with the GADM ID (integer) as the key and the ISO3 code (string) as
  the value."
  [[iso gadm-str]]
  {(read-string gadm-str) iso})

(def gadm-iso
  "Returns a single hash-map that associates all GADMs and ISO codes."
  (let [small-maps (map parse-line text-in)]
    (apply merge small-maps)))

(defn gadm->iso
  "Accepts an integer GADM ID and returns the associated ISO3 code as
  a string; wraps gadm-iso map for use in a cascalog query"
  [gadm]
  (gadm-iso gadm))
