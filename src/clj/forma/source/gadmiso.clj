;; This namespace associates a GADM ID (a subprovince-level unique
;; integer identifier) with a country-level ISO3 code.  The global
;; variable `gadm-iso` is bound to a large map with GADM IDs as keys
;; and ISO3 codes as values.  The supporting function `gadm->iso`
;; pulls out the ISO3 value of a supplied GADM ID for use within a
;; cascalog query.

;; Example: (gadm->iso 2048) => "AUS"

;; References:
;; GADM: http://www.gadm.org (level: ID_2)
;; ISO3: http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3

(ns forma.source.gadmiso
  (:use cascalog.api)
  (:require [clojure.string :as string]
            [clojure-csv.core :as csv]
            [forma.testing :as t]))

(def text-in
  "Reads `admin-map.csv` into a Clojure data structure, specifically a
  lazy sequence of vectors of the form [\"AFG\" \"1\"].  Note that the
  last line is empty, necessitating the use of `butlast`"
  (let [csv-path (t/project-path "/resources/admin-map.csv")]
    (butlast (csv/parse-csv (slurp csv-path)))))

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
