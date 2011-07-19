(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set)]
        [forma.source.static :only (static-tap)])
  (:require [forma.hadoop.io :as io]
            [forma.source.fire :as fire]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.load-tseries :as tseries]))

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

(gen-class :name forma.hadoop.jobs.GetStatic
           :prefix "get-static")

(defn get-static-main [out-path]
  (let [converter (forma/line->nums (hfs-textline *convert-path*))
        [vcf hansen ecoid gadm] (map static-tap
                                     [(static-chunktap "vcf")
                                      (static-chunktap "hansen")
                                      (hfs-seqfile *ecoid-path*)
                                      (hfs-seqfile *gadm-path*)])]
    (?<- (hfs-textline out-path :sinkparts 3)
         [?country ?mod-h ?mod-v ?sample ?line ?hansen ?ecoid ?vcf ?gadm]
         (vcf _ ?mod-h ?mod-v ?sample ?line ?vcf)
         (hansen _ ?mod-h ?mod-v ?sample ?line ?hansen)
         (ecoid _ ?mod-h ?mod-v ?sample ?line ?ecoid)
         (gadm _ ?mod-h ?mod-v ?sample ?line ?gadm)
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

(gen-class :name forma.hadoop.jobs.RunForma
           :prefix "run-forma")

;; Hardcoded in, for the big run.
(def *ndvi-path* "s3n://redddata/ndvi/1000-32/*/*/")
(def *rain-path* "s3n://redddata/precl/1000-32/*/*/")
(def *fire-path* "s3n://redddata/fire/1000-01/*/")

(defn forma-textline
  [path pathstr]
  (hfs-textline path
                :sink-template pathstr
                :outfields ["?text"]
                :templatefields ["?s-res" "?t-res" "?country" "?datestring"]
                :sinkparts 3))

;; TODO: Rewrite this, so that we only need to give it a sequence of
;; countries (or tiles), and it'll generate the rest.

(defn run-forma-main
  [out-path]
  (let [vcf-src     (static-chunktap "vcf")
        [ndvi-src rain-src] (map tseries/tseries-query
                                 [*ndvi-path* *rain-path*])
        country-src (forma/country-tap (hfs-seqfile *gadm-path*)
                                       (hfs-textline *convert-path*))
        fire-src (fire/fire-query "32" "2000-11-01" "2011-04-01" *fire-path*)]
    (?- (forma-textline out-path "%s-%s/%s/%s/")
        (forma/forma-query forma-map ndvi-src rain-src vcf-src country-src fire-src))))
