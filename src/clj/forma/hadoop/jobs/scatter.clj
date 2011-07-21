(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.utils :only (defjob)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)]
        [forma.source.tilesets :only (tile-set)])
  (:require [cascalog.ops :as c]
            [forma.hadoop.io :as io]
            [forma.source.modis :as m]
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
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?country]
      (gadm-src _ ?chunk)
      (convert-src ?line)
      (p/converter ?line :> ?country ?admin)
      (p/blossom-chunk ?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?admin)))

(defjob GetStatic
  [pail-path out-path]
  (let [[vcf hansen ecoid gadm] (map (comp static-tap
                                           split-chunk-tap)
                                     [["vcf"] ["hansen"] ["ecoid"] ["gadm"]])]
    (?<- (hfs-textline out-path
                       :sinkparts 3
                       :sink-template "%s/")
         [?country ?lat ?lon ?mod-h ?mod-v ?sample ?line ?hansen ?ecoid ?vcf ?gadm]
         (vcf    _ ?mod-h ?mod-v ?sample ?line ?vcf)
         (hansen _ ?mod-h ?mod-v ?sample ?line ?hansen)
         (ecoid  _ ?mod-h ?mod-v ?sample ?line ?ecoid)
         (gadm   _ ?mod-h ?mod-v ?sample ?line ?gadm)
         (m/modis->latlon ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
         (convert-line-src ?textline)
         (p/converter ?textline :> ?country ?gadm)
         (>= ?vcf 25))))

;; ## Forma

(def forma-map
  {:est-start "2005-12-01"
   :est-end "2011-04-01"
   :t-res "32"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 15
   :window 5})

(defn forma-textline
  [path pathstr]
  (hfs-textline path
                :sink-template pathstr
                :outfields ["?text"]
                :templatefields ["?s-res" "?t-res" "?country" "?datestring"]
                :sinkparts 3))

;; TODO: Rewrite this, so that we only need to give it a sequence of
;; countries (or tiles), and it'll generate the rest.

(defjob RunForma
  [pail-path ts-pail-path out-path]
  (?- (forma-textline out-path "%s-%s/%s/%s/")
      (forma/forma-query forma-map
                         (split-chunk-tap ts-pail-path ["ndvi"])
                         (split-chunk-tap ts-pail-path ["precl"])
                         (split-chunk-tap pail-path ["vcf"])
                         (country-tap (split-chunk-tap pail-path ["gadm"])
                                      convert-line-src)
                         (split-chunk-tap ts-pail-path ["fire"]))))

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
      (p/blossom-chunk ?rain-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?rain)
      (p/blossom-chunk ?gadm-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
      (c/avg ?rain :> ?avg-rain)))

(defjob ProcessRain
  [pail-path output-path]
  (?- (hfs-textline output-path)
      (rain-query (split-chunk-tap pail-path ["gadm"])
                  (split-chunk-tap pail-path ["rain"]))))
