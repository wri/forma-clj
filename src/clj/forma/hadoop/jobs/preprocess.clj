(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
        [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :only (tile-set)]
        [cascalog.io :only (with-fs-tmp)])
  (:require [forma.hadoop.predicate :as p]
            [forma.hadoop.io :as io]
            [forma.source.rain :as r]
            [forma.source.fire :as f]
            [forma.source.static :as s]
            [forma.static :as static]
            [cascalog.ops :as c]))

;; (defn rain-chunker
;;   "Like `modis-chunker`, for NOAA PRECL data files."
;;   [m-res chunk-size tiles source-path sink-path]
;;   (with-fs-tmp [fs tmp-dir]
;;     (let [file-tap (io/hfs-wholefile source-path)
;;           pix-tap (p/pixel-generator tmp-dir m-res tiles)
;;           ascii-map (:precl static/static-datasets)]
;;       (->> (r/rain-chunks m-res ascii-map chunk-size file-tap pix-tap)
;;            (to-pail sink-path)))))

(defmain PreprocessRain
  [source-path output-path s-res target-t-res]
  {:pre [(string? s-res)]}
  (let [nodata -9999.0
        t-res "32"
        rain-src (r/read-rain (static/static-datasets :precl) source-path)]
    (?- (hfs-seqfile output-path :sinkmode :replace)
        (r/rain-tap rain-src s-res nodata t-res target-t-res))))

(defmain ExplodePRECL
  "Process PRECL timeseries observations at native 0.5 degree resolution
   and expand each pixel into MODIS pixels at the supplied resolution"
  [in-path out-path s-res & iso-keys]
  (let [task-multiple 7 ;; rule of thumb based on experience
        tiles (apply tile-set iso-keys)
        num-tasks (* task-multiple (count tiles))
        src (hfs-seqfile in-path)
        out-loc (hfs-seqfile out-path :sinkmode :replace)]
    (with-job-conf {"mapred.map.tasks" 500}
      (r/exploder s-res tiles src))))

(defn static-chunker
  "m-res - MODIS resolution. "
  [m-res chunk-size tile-seq dataset agg ascii-path pail-path]
  (with-fs-tmp [_ tmp-dir]
    (let [line-tap (hfs-textline ascii-path)
          pix-tap  (p/pixel-generator tmp-dir m-res tile-seq)]
      (->> (s/static-chunks m-res chunk-size dataset agg line-tap pix-tap)
           (to-pail pail-path)))))

(defmain PreprocessStatic
  "See project wiki for example usage."
  [dataset ascii-path output-path s-res & countries]
  {:pre [(string? s-res)]}
  (static-chunker s-res
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
  [dataset ascii-path pail-path s-res & countries]
  {:pre [(#{"hansen" "vcf"} dataset)
         (string? s-res)]}
  (with-fs-tmp [_ tmp-dir]
    (let [line-tap (hfs-textline ascii-path)
          pix-tap  (->> countries
                        (map read-string)
                        (apply tile-set)
                        (p/pixel-generator tmp-dir s-res))]
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
more details. m-res is the resolution of the other MODIS data we are
using, likely \"500\""
  [type path pail-path m-res]
  (->> (case type
             "daily" (f/fire-source-daily     (hfs-textline path))
             "monthly" (f/fire-source-monthly (hfs-textline path)))
       (f/reproject-fires m-res)
       (to-pail pail-path)))
