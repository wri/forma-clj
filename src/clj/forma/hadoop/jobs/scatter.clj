(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [juke.utils :only (defmain)]
        [forma.source.tilesets :only (tile-set)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)])
  (:require [cascalog.ops :as c]
            [juke.reproject :as r]
            [forma.trends.stretch :as stretch]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]))

(def convert-line-src
  (hfs-textline "s3n://modisfiles/ascii/admin-map.csv"))

(defn static-tap
  "Accepts a source of DataChunks containing DoubleArrays or IntArrays
  as values, and returns a new query with all relevant spatial
  information plus the actual value."
  [chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (chunk-src _ ?chunk)
      (p/blossom-chunk ?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?val)))

(defn country-tap
  [gadm-src convert-src]
  (let [gadm-tap (static-tap gadm-src)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?country]
        (gadm-tap ?s-res ?mod-h ?mod-v ?sample ?line ?admin)
        (convert-src ?textline)
        (p/converter ?textline :> ?country ?admin))))

(defmain GetStatic
  [pail-path out-path]
  (let [[vcf hansen ecoid gadm border] (map (comp static-tap
                                                  (partial split-chunk-tap pail-path))
                                            [["vcf"] ["hansen"]
                                             ["ecoid"] ["gadm"]
                                             ["border"]])]
    (?<- (hfs-textline out-path
                       :sinkparts 3
                       :sink-template "%s/")
         [?country ?lat ?lon ?mod-h ?mod-v ?sample ?line ?hansen ?ecoid ?vcf ?gadm ?border]
         (vcf    ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
         (hansen ?s-res ?mod-h ?mod-v ?sample ?line ?hansen)
         (ecoid  ?s-res ?mod-h ?mod-v ?sample ?line ?ecoid)
         (gadm   ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
         (border ?s-res ?mod-h ?mod-v ?sample ?line ?border)
         (r/modis->latlon ?s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
         (convert-line-src ?textline)
         (p/converter ?textline :> ?country ?gadm)
         (>= ?vcf 25))))

;; ## Forma

(def forma-map
  {:est-start "2005-12-01"
   :est-end "2011-05-01"
   :s-res "1000"
   :t-res "32"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 15
   :window 5})

(defn paths-for-dataset
  [dataset s-res t-res]
  (let [res-string (format "%s-%s" s-res t-res)]
    (for [tile (tile-set :IDN :MYS)]
      [dataset res-string (apply r/hv->tilestring tile)])))

(defn constrained-tap
  [ts-pail-path dateset s-res t-res tile-seq]
  (apply split-chunk-tap
         ts-pail-path
         (paths-for-dataset "ndvi" s-res t-res)))

(defn adjusted-precl-tap
  [ts-pail-path s-res base-t-res t-res countries]
  (let [extracter (c/juxt #'io/extract-location
                          #'io/extract-chunk-value)
        combiner (c/comp #'io/mk-data-value
                         #'stretch/ts-expander)
        precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res countries)]
    (if (= t-res base-t-res)
      precl-tap
      (<- [?path ?final-chunk]
          (precl-tap ?path ?chunk)
          (extracter ?chunk :> ?location ?series)
          (combiner base-t-res t-res ?series :> ?new-series)
          (io/swap-data ?chunk ?new-series :> ?swapped-chunk)
          (io/set-temporal-res ?swapped-chunk t-res :> ?final-chunk)))))

(defmain RunForma
  "TODO: Rewrite this, so that we only need to give it a sequence of
  countries (or tiles), and it'll generate the rest."
  [pail-path ts-pail-path out-path & countries]
  (let [{:keys [s-res t-res]} forma-map
        countries (or countries [:IDN :MYS])]
    (?- (hfs-seqfile out-path)
        (forma/forma-query forma-map
                           (constrained-tap ts-pail-path "ndvi" s-res t-res countries)
                           (adjusted-precl-tap ts-pail-path s-res "32" t-res countries)
                           (constrained-tap pail-path "vcf" s-res "00" countries)
                           (country-tap (constrained-tap pail-path "gadm" s-res "00" countries)
                                        convert-line-src)
                           (tseries/fire-query pail-path
                                               t-res
                                               "2000-11-01"
                                               "2011-06-01"
                                               countries)))))

(defmain BucketForma
  [forma-path bucketed-path]
  (let [template-fields ["?s-res" "?country" "?datestring"]
        data-fields     ["?mod-h" "?mod-v" "?sample" "?line" "?text"]
        forma-fields    (concat template-fields data-fields)
        src (hfs-seqfile forma-path)]
    (?<- (hfs-textline bucketed-path
                       :sink-template "%s/%s/%s/"
                       :outfields data-fields
                       :templatefields template-fields
                       :sinkparts 3)
         forma-fields
         (src :>> forma-fields)
         ([[103] [158]] ?country :> true))))

;; ## Rain Processing, for Dan
;;
;; Dan wanted some means of generating the average rainfall per
;; administrative region. As we've already projected the data into
;; modis coordinates, we can simply take an average of all values per
;; administrative boundary; MODIS is far finer than gadm, so this will
;; mimic a weighted average.

(defn rain-query
  [gadm-src rain-src]
  (<- [?gadm ?date ?avg-rain]
      (rain-src _ ?rain-chunk)
      (gadm-src _ ?gadm-chunk)
      (io/extract-date ?rain-chunk :> ?date)
      (p/blossom-chunk ?rain-chunk :> _ ?mod-h ?mod-v ?sample ?line ?rain)
      (p/blossom-chunk ?gadm-chunk :> _ ?mod-h ?mod-v ?sample ?line ?gadm)
      (c/avg ?rain :> ?avg-rain)))

(defmain ProcessRain
  [pail-path output-path]
  (?- (hfs-textline output-path)
      (rain-query (split-chunk-tap pail-path ["gadm"])
                  (split-chunk-tap pail-path ["rain"]))))

(defmain GrabTimeseries
  [ts-pail-path output-path dataset position-seq]
  (let [positions (vec (read-string position-seq))
        src (split-chunk-tap ts-pail-path [dataset])
        extracter (c/comp #'io/get-pos #'io/extract-location)]
    (?<- (hfs-seqfile output-path)
         [?ts-chunk]
         (src _ ?ts-chunk)
         (extracter ?ts-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line)
         (positions ?mod-h ?mod-v ?sample ?line :> true))))
