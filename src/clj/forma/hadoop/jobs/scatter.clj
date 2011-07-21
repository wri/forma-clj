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

(defn static-chunktap
  [dataset]
  (io/chunk-tap "s3n://redddata/"
                dataset
                "1000-00"
                (for [[th tv] (vec (tile-set :IDN :MYS))]
                  (format "%03d%03d" th tv))))

(def *gadm-path* "s3n://redddata/gadm/1000-00/*/*/")
(def *ecoid-path* "s3n://redddata/ecoid/1000-00/*/*/")
(def *convert-path* "s3n://modisfiles/ascii/admin-map.csv")
(def *ndvi-path* "s3n://redddata/ndvi/1000-32/*/*/")
(def *rain-path* "s3n://redddata/precl/1000-32/*/*/")
(def *fire-path* "s3n://redddata/fire/1000-01/*/")

(defjob GetStatic
  [pail-path out-path]
  (let [converter (forma/line->nums (hfs-textline *convert-path*))
        [vcf hansen ecoid gadm] (map split-chunk-tap
                                     [["vcf"] ["hansen"] ["ecoid"] ["gadm"]])]
    (?<- (hfs-textline out-path :sinkparts 3 :sink-template "%s/")
         [?country ?lat ?lon ?mod-h ?mod-v ?sample ?line ?hansen ?ecoid ?vcf ?gadm]
         (vcf _ ?mod-h ?mod-v ?sample ?line ?vcf)
         (hansen _ ?mod-h ?mod-v ?sample ?line ?hansen)
         (ecoid _ ?mod-h ?mod-v ?sample ?line ?ecoid)
         (gadm _ ?mod-h ?mod-v ?sample ?line ?gadm)
         (m/modis->latlon ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
         (converter ?country ?gadm)
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
  [out-path]
  (let [[ndvi-src rain-src] (map tseries/tseries-query
                                 [*ndvi-path* *rain-path*])
        country-src (forma/country-tap (hfs-seqfile *gadm-path*)
                                       (hfs-textline *convert-path*))
        vcf-src  (static-chunktap "vcf")
        fire-src (tseries/fire-query "32" "2000-11-01" "2011-04-01" *fire-path*)]
    (?- (forma-textline out-path "%s-%s/%s/%s/")
        (forma/forma-query forma-map ndvi-src rain-src vcf-src country-src fire-src))))

;; ## Rain Processing, for Dan
;;
;; This can probably go into its own project.

(defn rain-tap
  "TODO: Very similar to extract-tseries. Consolidate."
  [rain-src]
  (<- [?mod-h ?mod-v ?sample ?line ?date ?val]
      (rain-src _ ?s-res _ ?tilestring ?date ?chunkid ?chunk)
      (io/count-vals ?chunk :> ?chunk-size)
      (p/struct-index 0 ?chunk :> ?pix-idx ?val)
      (m/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
      (m/tile-position ?s-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defn run-rain
  [gadm-src rain-src]
  (let [gadm-src (static-tap gadm-src)
        rain-src (rain-tap rain-src)]
    (<- [?gadm ?date ?avg-rain]
        (gadm-src _ ?mod-h ?mod-v ?sample ?line ?gadm)
        (rain-src ?mod-h ?mod-v ?sample ?line ?date ?rain)
        (c/avg ?rain :> ?avg-rain))))

(defjob ProcessRain
  [path]
  (?- (hfs-textline path)
      (run-rain (hfs-seqfile *gadm-path*)
                (hfs-seqfile *rain-path*))))

;; this guy really exists only to transfer the old processed static
;; chunks over to the new SplitDataChunkPailStructure.

(defjob StaticPailer
  [pattern]
  (let [static-src (hfs-seqfile "s3n://redddata/"
                                :source-pattern pattern)
        chunkifier (io/chunkify 24000)]
    (?pail- (split-chunk-tap "s3n://pailbucket/rawstore/")
            (<- [?datachunk]
                (static-src ?dataset ?s-res ?t-res ?tilestring ?chunkid ?chunk)
                (p/add-fields nil :> !date)
                (m/tilestring->hv ?tilestring :> ?mod-h ?mod-v)
                (chunkifier ?dataset !date ?s-res ?t-res
                            ?mod-h ?mod-v ?chunkid ?chunk :> ?datachunk)))))
