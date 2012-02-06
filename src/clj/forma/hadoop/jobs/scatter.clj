(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)])
  (:require [cascalog.ops :as c]
            [forma.utils :only (throw-illegal)]
            [forma.reproject :as r]
            [forma.schema :as schema]
            [forma.trends.stretch :as stretch]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]))

(def convert-line-src
  (hfs-textline "s3n://modisfiles/ascii/admin-map.csv"))

(defn static-tap
  "Accepts a source of DataChunks containing vectors as values, and
  returns a new query with all relevant spatial information plus the
  actual value."
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
(def forma-run-parameters
  {"1000-32" {:est-start "2005-12-31"
              :est-end "2011-08-01" ;; I KEEP FUCKING THIS UP
              :s-res "1000"
              :t-res "32"
              :neighbors 1
              :window-dims [600 600]
              :vcf-limit 25
              :long-block 15
              :window 5}
   "1000-16" {:est-start "2005-12-31"
              :est-end "2011-08-01"
              :s-res "1000"
              :t-res "16"
              :neighbors 1
              :window-dims [600 600]
              :vcf-limit 25
              :long-block 30
              :window 10
              :ridge-const 1e-8
              :convergence-thresh 1e-6
              :max-iterations 500}})

(defn paths-for-dataset
  [dataset s-res t-res tile-seq]
  (let [res-string (format "%s-%s" s-res t-res)]
    (for [tile tile-seq]
      [dataset res-string (apply r/hv->tilestring tile)])))

(defn constrained-tap
  [ts-pail-path dataset s-res t-res country-seq]
  (->> (apply tile-set country-seq)
       (paths-for-dataset dataset s-res t-res)
       (apply split-chunk-tap ts-pail-path)))

(defn adjusted-precl-tap
  "Document... returns a tap that adjusts for the incoming
  resolution."
  [ts-pail-path s-res base-t-res t-res country-seq]
  (let [precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res country-seq)]
    (if (= t-res base-t-res)
      precl-tap
      (<- [?path ?final-chunk]
          (precl-tap ?path ?chunk)
          (map ?chunk [:location :value] :> ?location ?series)
          (stretch/ts-expander base-t-res t-res ?series :> ?new-series)
          (assoc ?chunk
            :value ?new-series
            :temporal-res t-res :> ?final-chunk)))))

(defn first-half-query
  "Poorly named! Returns a query that generates a number of position and dataset identifier"
  [pail-path ts-pail-path out-path run-key country-seq]
  (let [{:keys [s-res t-res] :as forma-map} (forma-run-parameters run-key)]
    (assert forma-map (str run-key " is not a valid run key!"))
    (forma/forma-query forma-map
                       (constrained-tap ts-pail-path "ndvi" s-res t-res country-seq)
                       (constrained-tap ts-pail-path "reli" s-res t-res country-seq)
                       (adjusted-precl-tap ts-pail-path s-res "32" t-res country-seq)
                       (constrained-tap pail-path "vcf" s-res "00" country-seq)
                       (tseries/fire-query pail-path
                                           t-res
                                           "2000-11-01"
                                           "2011-06-01"
                                           country-seq))))
