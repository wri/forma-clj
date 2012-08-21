(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
        [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :as t]
        [cascalog.io :only (with-fs-tmp)])
  (:require [forma.hadoop.predicate :as p]
            [forma.hadoop.io :as io]
            [forma.source.rain :as r]
            [forma.source.fire :as f]
            [forma.source.static :as s]
            [forma.static :as static]
            [cascalog.ops :as c]))

(defmain PreprocessStatic
  "Use to process all static datasets. See the project wiki for
   details on getting the correct files in place on HDFS.

   Note that the Hansen and VCF datasets are handled differently
   because they line up perfectly with the MODIS tiling scheme.

   Locations can be specified as iso code keywords (e.g. :BRA :MEX) or
   string tile vectors (e.g. \"[28 8]\" \"[29 9]\").

   Usage:
     hadoop jar forma-0.2.0-SNAPSHOT-standalone.jar \\
                forma.hadoop.jobs.preprocess.PreprocessStatic \\
                \"gadm\" \\
                \"user/hadoop/border.txt\" \\
                \"s3n://bucketname/pail-loc/\" \\
                \"500\" \\
                :VEN"
  [dataset ascii-path pail-path s-res & locations]
  {:pre [(string? s-res)
         (not (nil? locations))]}
  (with-fs-tmp [_ tmp-dir]
    (let [tile-seq (if (= :all (read-string (first locations)))
                     (apply t/tile-set (keys t/country-tiles))
                     (apply t/tile-set (map read-string locations))) 
          line-tap (hfs-textline ascii-path)
          pix-tap  (p/pixel-generator tmp-dir s-res tile-seq)
          agg ({"vcf" c/min "hansen" c/sum} dataset c/max)
          chunker ({"vcf" s/static-modis-chunks
                    "hansen" s/static-modis-chunks}
                   dataset s/static-chunks)]
      (->> (if (and (#{"hansen" "vcf"} dataset)
                    (= "500" s-res))
             (chunker static/chunk-size dataset agg line-tap pix-tap)
             (chunker s-res static/chunk-size dataset agg line-tap pix-tap))
           (to-pail pail-path)))))

(defn rain-chunker
  "Like `modis-chunker`, for NOAA PRECL data files."
  [m-res chunk-size tiles source-path sink-path]
  (with-fs-tmp [fs tmp-dir]
    (let [file-tap (io/hfs-wholefile source-path)
          pix-tap (p/pixel-generator tmp-dir m-res tiles)
          ascii-map (:precl static/static-datasets)]
      (->> (r/rain-chunks m-res ascii-map chunk-size file-tap pix-tap)
           (to-pail sink-path)))))

(defmain PreprocessRain
  "The locations can be ISO keywords or [h v] tuples."
  [source-path sink-path s-res & locations]
  {:pre [(string? s-res)
         locations]}
  (let [tiles (apply t/tile-set (map read-string locations))
        chunk-size static/chunk-size]
    (rain-chunker s-res chunk-size tiles source-path sink-path)))

;; ## Fires Processing
;;
;; Note that the preprocessing performed by `fire-chunker` is going to
;; aggregate fires into daily buckets; we won't be able to get any
;; other information. We should probably change this in future to
;; retain as much information as possible.

(defmain PreprocessFire
  "Path for running FORMA fires processing. See the forma-clj wiki for
more details. m-res is the resolution of the other MODIS data we are
using, likely \"500\""
  [type path pail-path m-res]
  (->> (case type
             "daily" (f/fire-source-daily     (hfs-textline path))
             "monthly" (f/fire-source-monthly (hfs-textline path)))
       (f/reproject-fires m-res)
       (to-pail pail-path)))
