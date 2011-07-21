(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.utils :only (defjob)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)]
        [forma.source.tilesets :only (tile-set)]
        [forma.source.static :only (static-tap)])
  (:require [cascalog.ops :as c]
            [forma.hadoop.io :as io]
            [forma.source.modis :as m]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]))

(def *convert-path* "s3n://modisfiles/ascii/admin-map.csv")

(defjob GetStatic
  [pail-path out-path]
  (let [converter (forma/line->nums (hfs-textline *convert-path*))
        [vcf hansen ecoid gadm] (map (comp static-tap
                                           split-chunk-tap)
                                     [["vcf"] ["hansen"] ["ecoid"] ["gadm"]])]
    (?<- (hfs-textline out-path
                       :sinkparts 3
                       :sink-template "%s/")
         [?country ?lat ?lon ?mod-h ?mod-v ?sample ?line ?hansen ?ecoid ?vcf ?gadm]
         (vcf _ ?mod-h ?mod-v ?sample ?line ?vcf)
         (hansen _ ?mod-h ?mod-v ?sample ?line ?hansen)
         (ecoid _ ?mod-h ?mod-v ?sample ?line ?ecoid)
         (gadm _ ?mod-h ?mod-v ?sample ?line ?gadm)
         (m/modis->latlon ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
         (converter ?country ?gadm)
         (>= ?vcf 25))))

;; ## Forma

(def *gadm-path* "s3n://redddata/gadm/1000-00/*/*/")
(def *ndvi-path* "s3n://redddata/ndvi/1000-32/*/*/")
(def *rain-path* "s3n://redddata/precl/1000-32/*/*/")
(def *fire-path* "s3n://redddata/fire/1000-01/*/")

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
  [pail-path out-path]
  (let [[ndvi-src rain-src] (map tseries/tseries-query [*ndvi-path* *rain-path*])
        country-src (forma/country-tap (hfs-seqfile *gadm-path*)
                                       (hfs-textline *convert-path*))
        vcf-src  (split-chunk-tap pail-path ["vcf"])
        fire-src (tseries/fire-query "32" "2000-11-01" "2011-04-01" *fire-path*)]
    (?- (forma-textline out-path "%s-%s/%s/%s/")
        (forma/forma-query forma-map ndvi-src rain-src vcf-src country-src fire-src))))

;; ## Rain Processing, for Dan
;;
;; Dan wanted some means of generating the average rainfall per
;; administrative region. As we've already projected the data into
;; modis coordinates, we can simply take an average of all values per
;; administrative boundary; MODIS is far finer than gadm, so this will
;; mimic a weighted average.

(defn rain-tap
  "TODO: Very similar to extract-tseries. Consolidate."
  [rain-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?date ?val]
      (rain-src _ ?chunk)
      ((c/juxt #'io/extract-chunk-value
               #'io/extract-location
               #'io/extract-date) ?chunk :> ?rain-chunk ?location ?date)
      (p/struct-index 0 ?rain-chunk :> ?pix-idx ?val)
      (io/get-pos ?location ?pix-idx :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn run-rain
  [gadm-src rain-src]
  (let [gadm-src (static-tap gadm-src)
        rain-src (rain-tap rain-src)]
    (<- [?gadm ?date ?avg-rain]
        (gadm-src _ ?mod-h ?mod-v ?sample ?line ?gadm)
        (rain-src _ ?mod-h ?mod-v ?sample ?line ?date ?rain)
        (c/avg ?rain :> ?avg-rain))))

(defjob ProcessRain
  [pail-path output-path]
  (?- (hfs-textline output-path)
      (run-rain (split-chunk-tap pail-path ["gadm"])
                (split-chunk-tap pail-path ["rain"]))))
