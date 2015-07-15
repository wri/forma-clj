(ns forma.hadoop.jobs.update
  "data update workflow."
  (:use cascalog.api)
  (:require [cascalog.checkpoint :refer [workflow]]
            [forma.hadoop.jobs.preprocess :as p]
            [forma.hadoop.jobs.runner :as r]
            [forma.hadoop.jobs.timeseries :as ts]))

(defn config
  "Configuration for FORMA update runs."
  [est-start est-end run-date]
  (let [training-end "2005-12-19"]
    {:configurable
     {:threshold 50
      :version "1.0"
      :spatial-res "500"
      :temporal-res "16"
      :cdm-temporal-res "32"
      :modis-layers [:ndvi]
      :tiles [:all]
      :rundate run-date
      :training-end training-end
      :est-start est-start
      :est-end est-end
      :fire-start "2000-11-01"
      :super-eco? false
      :nodata -9999.0
      :zoom 17
      :min-zoom 6}
     :storage
     {:tmp "/tmp"
      :staging "s3n://formastaging"
      :static "s3n://pailbucket/all-static-seq/all"
      :gadm2 "s3n://pailbucket/all-static-seq/vcf-filtered/gadm2"
      :archive "s3n://modisfiles"
      :s3out (str "s3n://pailbucket/output/run-" run-date)
      :pailpath "s3n://pailbucket/all-master"
      :betas "s3n://pailbucket/all-betas"
      :gadm2eco "$S3OUT/merged-estimated-gadm2-eco"
      :blueraster (format "s3n://wriforma/%s/%s-to-%s"
                          run-date
                          training-end
                          est-end)}}))

(defmain RunUpdates
  [est-start est-end run-date]
  (let [{conf :configurable storage :storage} (config est-start est-end run-date)
        path (fn [k & ss] (apply str (k storage) "/" ss))
        adjusted-s3         (path :s3out "adjusted")
        merged-trends-s3    (path :s3out "merged-trends")
        beta-s3             (path :s3out "betas")
        estimated-s3        (path :s3out "estimated")
        merged-estimated-s3 (path :s3out "merged-estimated")
        gfw-site-s3         (path :s3out "gfw-site")
        forma-site-s3       (path :s3out "forma-site-" (:threshold conf))
        david-s3            (path :s3out "david")]
    (workflow
     ["/tmp"]
     start-workflow
     ([] (println "Starting workflow."))

     ;; preprocess-modis
     ;; ([]
     ;;  (println "Preprocessing MODIS data")
     ;;  (p/PreprocessModis (path :staging "MOD13A1/")
     ;;                     (:pailpath storage)
     ;;                     "{20}*"
     ;;                     (:tiles conf)
     ;;                     (:modis-layers conf)))

     ;; pre-rain
     ;; ([:tmp-dirs rain-output
     ;;   :deps start-workflow]
     ;;  (println "Preprocessing rain")
     ;;  (p/PreprocessRain (path :archive "PRECL")
     ;;                    rain-output
     ;;                    (:spatial-res conf)
     ;;                    (:temporal-res conf)))

     ;; rain->modis
     ;; ([:tmp-dirs exploded-rain-output
     ;;   :deps pre-rain]
     ;;  (println "Exploding rain into MODIS pixels")
     ;;  (p/ExplodeRain rain-output
     ;;                 exploded-rain-output
     ;;                 (:spatial-res conf)
     ;;                 (:tiles conf)))

     ;; ndvi-timeseries
     ;; ([:tmp-dirs ndvi-series-output
     ;;   :deps start-workflow]
     ;;  (println "NDVI timeseries")
     ;;  (ts/ModisTimeseries (:pailpath storage)
     ;;                      ndvi-series-output
     ;;                      (:spatial-res conf)
     ;;                      (:temporal-res conf)
     ;;                      "ndvi"))

     ;; ndvi-filter
     ;; ([:tmp-dirs filtered-ndvi-output
     ;;   :deps ndvi-timeseries]
     ;;  (println "Filtering NDVI")
     ;;  (r/TimeseriesFilter (:spatial-res conf)
     ;;                      (:temporal-res conf)
     ;;                      ndvi-series-output
     ;;                      (:static storage)
     ;;                      filtered-ndvi-output))

     ;; adjust-series
     ;; ([:deps [rain->modis ndvi-filter]]
     ;;  (println "Joining NDVI and rain series, adjusting length")
     ;;  (r/AdjustSeries (:spatial-res conf)
     ;;                  (:temporal-res conf)
     ;;                  filtered-ndvi-output
     ;;                  exploded-rain-output
     ;;                  adjusted-s3))

     ;; trends
     ;; ([:tmp-dirs trends-output
     ;;   :deps adjust-series]
     ;;  (println "Calculating trends stats")
     ;;  (r/Trends (:spatial-res conf)
     ;;            (:temporal-res conf)
     ;;            est-end
     ;;            adjusted-s3
     ;;            trends-output
     ;;            est-start))

     ;; trends->pail
     ;; ([:deps trends]
     ;;  (println "Adding trends series to pail")
     ;;  (r/TrendsPail (:spatial-res conf)
     ;;                (:temporal-res conf)
     ;;                est-end
     ;;                trends-output
     ;;                (:pailpath storage)))

     merge-trends
     ([
       ;; :deps trends->pail
       ]
      (println "Merging trends time series stored in pail")
      (r/MergeTrends (:spatial-res conf)
                     (:temporal-res conf)
                     est-end
                     (:pailpath storage)
                     merged-trends-s3))

     preprocess-fires
     ([:tmp-dirs fire-output
       :deps start-workflow]
      (println "Preprocessing fires")
      (p/PreprocessFire (path :archive "/fires")
                        fire-output
                        "500" "16"
                        (:fire-start conf)
                        (:tiles conf)))

     forma-tap
     ([:tmp-dirs forma-tap-output
       :deps [preprocess-fires merge-trends]]
      (println "Prepping FORMA tap for neighbor analysis")
      (r/FormaTap (:spatial-res conf)
                  (:temporal-res conf)
                  est-start
                  est-end
                  fire-output
                  merged-trends-s3
                  forma-tap-output))

     neighbors
     ([:tmp-dirs neighbors-output
       :deps forma-tap]
      (println "Merging neighbors")
      (r/NeighborQuery (:spatial-res conf)
                       (:temporal-res conf)
                       forma-tap-output
                       neighbors-output))

     beta-data-prep
     ([:tmp-dirs beta-data-output
       :deps neighbors]
      (println "Beta data prep - only keep data through training period")
      (r/BetaDataPrep (:spatial-res conf)
                      (:temporal-res conf)
                      neighbors-output
                      (:static storage)
                      beta-data-output
                      true))

     ;; gen-betas
     ;; ([:deps beta-data-prep]
     ;;  (println "Generating beta vectors")
     ;;  (r/GenBetas (:spatial-res conf)
     ;;              (:temporal-res conf)
     ;;              (:training-end conf)
     ;;              beta-data-output
     ;;              beta-s3))

     forma-estimate
     ([:deps neighbors]
      (println "Classify pixels using beta vectors")
      (r/EstimateForma (:spatial-res conf)
                       (:temporal-res conf)
                       (:betas storage)
                       neighbors-output
                       (:static storage)
                       estimated-s3
                       (:super-eco? conf)))

     probs-pail
     ([:deps forma-estimate]
      (println "Add probability output to pail")
      (r/ProbsPail (:spatial-res conf)
                   (:temporal-res conf)
                   est-end
                   estimated-s3
                   (:pailpath storage)))

     merge-probs
     ([:deps probs-pail]
      (println "Merge probability time series.")
      (r/MergeProbs (:spatial-res conf)
                    (:temporal-res conf)
                    est-end
                    (:pailpath storage)
                    merged-estimated-s3))

     gadm2-eco-ids
     ([:deps merge-probs]
      (println "Merging in gadm2 and eco fields")
      (r/ProbsGadm2 merged-estimated-s3
                    (:gadm2 storage)
                    (:static storage)
                    (:gadm2eco storage)))

     common-data-conversion
     ([:deps gadm2-eco-ids]
      (println "Prepping for website")
      (r/FormaWebsite (:threshold conf)
                      (:zoom conf)
                      (:min-zoom conf)
                      (:spatial-res conf)
                      (:temporal-res conf)
                      (:cdm-temporal-res conf)
                      (:training-end conf)
                      (:nodata conf)
                      (:gadm2eco storage)
                      gfw-site-s3))

     download-prep
     ([:deps gadm2-eco-ids]
      ;; # prep data for FORMA download
      (println "Prepping data for FORMA download link")
      (r/FormaDownload (:threshold conf)
                       (:spatial-res conf)
                       (:temporal-res conf)
                       (:nodata conf)
                       (:gadm2eco storage)
                       forma-site-s3))

     blue-raster
     ([:deps gadm2-eco-ids]
      (println "Converting for Blue Raster")
      (r/BlueRaster (:spatial-res conf)
                    (:temporal-res conf)
                    (:nodata conf)
                    (:gadm2eco storage)
                    (:static storage)
                    (:blueraster storage)))

     david-conversion
     ([:deps gadm2-eco-ids]
      (println "Converting for David")
      (r/FormaDavid (:nodata conf)
                    (:gadm2eco storage)
                    (:static storage)
                    david-s3)))))
