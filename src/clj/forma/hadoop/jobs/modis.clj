(ns forma.hadoop.jobs.modis
  (:use [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :only (tile-set)])
  (:require [forma.hadoop.io :as io]
            [forma.source.hdf :as h]
            [forma.static :as static])
  (:gen-class))

(defn modis-chunker
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets chunk-size in-path pattern pail-path]
  {:pre (seq subsets)}
  (let [source (io/hfs-wholefile in-path :source-pattern pattern)]
    (->> (h/modis-chunks subsets chunk-size source)
         (to-pail pail-path))))

(defn -main
  "See project wiki for example usage."
  [path pail-path date & tiles]
  (let [pattern (->> tiles
                     (map read-string)
                     (apply tile-set)
                     (apply io/tiles->globstring)
                     (str date "/"))]
    (modis-chunker static/forma-subsets
                   static/chunk-size
                   path
                   pattern
                   pail-path)))
