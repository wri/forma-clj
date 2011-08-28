(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
        [juke.utils :only (defmain)]
        [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :only (tile-set)]
        [cascalog.io :only (with-fs-tmp)])
  (:require [forma.hadoop.predicate :as p]
            [forma.hadoop.io :as io]
            [forma.source.hdf :as h]
            [forma.source.rain :as r]
            [forma.source.fire :as f]
            [forma.source.static :as s]
            [forma.static :as static]
            [cascalog.ops :as c]))

(defn modis-chunker
  "Cascalog job that takes set of dataset identifiers, a chunk size, a
  directory containing MODIS HDF files, or a link directly to such a
  file, and an output dir, harvests tuples out of the HDF files, and
  sinks them into a custom directory structure inside of
  `output-dir`."
  [subsets chunk-size in-path pattern pail-path]
  (let [source (io/hfs-wholefile in-path :source-pattern pattern)]
    (->> (h/modis-chunks subsets chunk-size source)
         (to-pail pail-path))))

(defmain PreprocessModis
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

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res chunk-size tile-seq path pail-path]
  (with-fs-tmp [fs tmp-dir]
    (let [file-tap (io/hfs-wholefile path)
          pix-tap (p/pixel-generator tmp-dir m-res tile-seq)
          ascii-map (:precl static/static-datasets)]
      (->> (r/rain-chunks m-res ascii-map chunk-size file-tap pix-tap)
           (to-pail pail-path)))))

(defmain PreprocessRain
  "See project wiki for example usage."
  [path pail-path & countries]
  (let [countries (map read-string countries)]
    (rain-chunker "1000"
                  static/chunk-size
                  (apply tile-set countries)
                  path
                  pail-path)))

(defn static-chunker
  [m-res chunk-size tile-seq dataset agg ascii-path pail-path]
  (with-fs-tmp [fs tmp-dir]
    (let [line-tap (hfs-textline ascii-path)
          pix-tap  (p/pixel-generator tmp-dir m-res tile-seq)]
      (->> (s/static-chunks m-res chunk-size dataset agg line-tap pix-tap)
           (to-pail pail-path)))))

(defmain PreprocessStatic
  "See project wiki for example usage."
  [dataset ascii-path output-path & countries]
  (static-chunker "1000"
                  static/chunk-size
                  (->> countries
                       (map read-string)
                       (apply tile-set))
                  dataset
                  ({"vcf" c/max "hansen" c/sum} dataset c/max)
                  ascii-path
                  output-path))

(defmain PreprocessAscii
  "TODO: This is only good for hansen datasets looking to be combined
  Tidy up. This needs to be combined with PreprocessStatic."
  [dataset ascii-path pail-path & countries]
  {:pre [(#{"hansen" "vcf"} dataset)]}
  (with-fs-tmp [_ tmp-dir]
    (let [line-tap (hfs-textline ascii-path)
          pix-tap  (->> countries
                        (map read-string)
                        (apply tile-set)
                        (p/pixel-generator tmp-dir "1000"))]
      (->> (s/static-modis-chunks static/chunk-size
                                  dataset
                                  ({"vcf" c/min "hansen" c/sum} dataset c/max)
                                  line-tap
                                  pix-tap)
           (to-pail pail-path)))))

;; ## Fires Processing
;;
;; Note that the preprocessing performed by `fire-chunker` is going to
;; aggregate fires into daily buckets; we won't be able to get any
;; other information. We should probably change this in future to
;; retain as much information as possible.

(defmain PreprocessFire
  "Path for running FORMA fires processing. See the forma-clj wiki for
more details."
  [type path pail-path]
  (->> (case type
             "daily" (f/fire-source-daily     (hfs-textline path))
             "monthly" (f/fire-source-monthly (hfs-textline path)))
       (f/reproject-fires "1000")
       (to-pail pail-path)))
