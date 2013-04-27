(ns forma.hadoop.jobs.preprocess
  (:use cascalog.api
        [forma.hadoop.pail :only (to-pail split-chunk-tap)]
        [forma.source.tilesets :only (tile-set)]
        [cascalog.io :only (with-fs-tmp)])
  (:require [forma.hadoop.predicate :as p]
            [forma.hadoop.io :as io]
            [forma.source.rain :as r]
            [forma.reproject :as reproj]
            [forma.thrift :as thrift]
            [forma.hadoop.jobs.forma :as forma]
            [forma.source.fire :as f]
            [forma.source.static :as s]
            [forma.hadoop.predicate :as p]
            [forma.source.hdf :as hdf]
            [forma.hadoop.jobs.timeseries :as tseries]
            [forma.static :as static]
            [cascalog.ops :as c]
            [forma.utils :as utils]))

(defn parse-locations
  "Parse collection of tile vectors or iso codes into set of tiles. Tiles
   and iso codes are typically supplied to defmains via the command line,
   and may therefore be strings."
  [tiles-or-isos]
  {:pre [(let [tiles-or-isos (utils/arg-parser tiles-or-isos)] ;; handle str
           (or (= :all tiles-or-isos) ;; check for :all kw
               (every? #(or (coll? %) (keyword? %)) tiles-or-isos)))] 
   :post [(let [not-kw? (comp not keyword? first)] ;; handle incorrect nesting
            (every? not-kw? %))]}
  (let [tiles-or-isos (utils/arg-parser tiles-or-isos) ;; handle string colls
        tiles-or-isos (if (coll? tiles-or-isos) ;; handle single element
                        tiles-or-isos
                        [tiles-or-isos])]
    (->> (utils/arg-parser tiles-or-isos)
         (apply tile-set))))

(defmain PreprocessRain
  [source-path output-path s-res target-t-res]
  {:pre [(string? s-res)]}
  (let [nodata -9999.0
        t-res "32"
        rain-src (r/read-rain (static/static-datasets :precl) source-path)]
    (?- (hfs-seqfile output-path :sinkmode :replace)
        (r/rain-tap rain-src s-res nodata t-res target-t-res))))

(defmain ExplodeRain
  "Process PRECL timeseries observations at native 0.5 degree resolution
   and expand each pixel into MODIS pixels at the supplied resolution.

   `task-multiple` and `num-tasks` are used to ensure that output
    files aren't gigantic. With `task-multiple` set to 15, processing
    80 tiles results in 1200 map tasks, and output file sizes max out
    at about 600mb. With a smaller task-multiple, files could reach several
    gigabytes. Although this size is normally ok with S3, random transfer
    errors take a while to recover from with files that large."
  [in-path out-path s-res tiles-or-isos]
  (let [task-multiple 15
        tiles (parse-locations tiles-or-isos)
        num-tasks (* task-multiple (count tiles))
        src (hfs-seqfile in-path)
        out-loc (hfs-seqfile out-path :sinkmode :replace)]
    (with-job-conf {"mapred.map.tasks" num-tasks}
      (?- out-loc (r/exploder s-res tiles src)))))

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

(defmain ExplodeStatic
  "Explode DataChunks from a pail into individual pixels."
  [dataset s-res pail-path out-path]
  (let [src (split-chunk-tap pail-path [dataset (format "%s-00" s-res)])
        sink (hfs-seqfile out-path)]
    (?<- sink [?s-res ?mod-h ?mod-v ?sample ?line ?val]
         (src _ ?dc)
         (thrift/unpack ?dc :> _ ?tile-loc ?data _ _ _)
         (thrift/unpack* ?data :> ?data-value)
         (p/index ?data-value :> ?pixel-idx ?val)
         (thrift/unpack ?tile-loc :> ?s-res ?mod-h ?mod-v ?id ?size)
         (reproj/tile-position ?s-res ?size ?id ?pixel-idx :> ?sample ?line))))

;; ## Fires Processing
;;
;; Note that the preprocessing performed by `fire-chunker` is going to
;; aggregate fires into daily buckets; we won't be able to get any
;; other information. We should probably change this in future to
;; retain as much information as possible.

(defmain PreprocessFire
  "Path for running FORMA fires processing. See the forma-clj wiki for
   more details. m-res is the desired output resolution, likely the
   resolution of the other MODIS data we are using (i.e. \"500\")"
  ([path out-path m-res out-t-res start-date est-start est-end tiles-or-isos]
     (let [tiles (parse-locations tiles-or-isos)
           fire-src (f/fire-source (hfs-textline path) tiles m-res)
           reproject-query (f/reproject-fires m-res fire-src)
           ts-query (tseries/fire-query reproject-query m-res out-t-res start-date est-start est-end)
           adjusted-fires (forma/fire-tap est-start est-end out-t-res ts-query)]
       (?- (hfs-seqfile out-path :sinkmode :replace) adjusted-fires))))

(defmain PreprocessModis
  "Preprocess MODIS data from raw HDF files to pail.

   Usage:
     hadoop jar target/forma-0.2.0-SNAPSHOT-standalone.jar s3n://formastaging/MOD13A1 \\
     s3n://pailbucket/all-master \"{2012}*\" \"[:all]\""
  [input-path pail-path date tiles-or-isos subsets]  
  (let [subsets (->> (utils/arg-parser subsets)
                     (filter (partial contains? (set static/forma-subsets))))
        tiles (parse-locations tiles-or-isos)
        pattern (->> tiles
                     (apply io/tiles->globstring)
                     (str date "/"))]
    (hdf/modis-chunker subsets static/chunk-size input-path pattern pail-path)))
