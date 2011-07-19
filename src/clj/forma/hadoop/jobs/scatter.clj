(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set)]
        [forma.source.static :only (static-tap)])
  (:require [cascalog.ops :as c]
            [forma.hadoop.io :as io]
            [forma.source.fire :as fire]
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

(gen-class :name forma.hadoop.jobs.GetStatic
           :prefix "get-static-")

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
           :prefix "run-forma-")

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
  (let [
        [ndvi-src rain-src] (map tseries/tseries-query
                                 [*ndvi-path* *rain-path*])
        country-src (forma/country-tap (hfs-seqfile *gadm-path*)
                                       (hfs-textline *convert-path*))
        vcf-src  (static-chunktap "vcf")
        fire-src (fire/fire-query "32" "2000-11-01" "2011-04-01" *fire-path*)]
    (?- (forma-textline out-path "%s-%s/%s/%s/")
        (forma/forma-query forma-map ndvi-src rain-src vcf-src country-src fire-src))))

;; ## Fires Processing

(gen-class :name forma.hadoop.jobs.ProcessFires
           :prefix "process-fires-")

(defn process-fires-main
  "Path for running FORMA fires processing. See the forma-clj wiki for
more details."
  [type path out-dir]
  (?- (io/chunk-tap out-dir "%s/%s-%s/")
      (->> (case type
                 "daily" (fire/fire-source-daily     (hfs-textline path))
                 "monthly" (fire/fire-source-monthly (hfs-textline path)))
           (fire/reproject-fires "1000"))))

;; ## Rain Processing, for Dan

(gen-class :name forma.hadoop.jobs.ProcessRain
           :prefix "process-rain-")

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

(defn process-rain-main [path]
  (?- (hfs-textline path)
      (run-rain (hfs-seqfile *gadm-path*)
                (hfs-seqfile *rain-path*))))
