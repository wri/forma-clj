(ns forma.hadoop.jobs.runner
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set country-tiles)])
  (:require [forma.hadoop.jobs.forma :as forma]
            [forma.utils :as utils]))

(defn run-params [k est-end]
  (-> {"500-16" {:est-start "2005-12-19"
                 :est-end est-end
                 :s-res "500"
                 :t-res "16"
                 :neighbors 1
                 :window-dims [600 600]
                 :vcf-limit 25
                 :long-block 30
                 :window 10
                 :ridge-const 1e-8
                 :convergence-thresh 1e-6
                 :max-iterations 500
                 :min-coast-dist 3
                 :nodata -9999.0}}
      (get k)))

(defn get-est-map
  [s-res t-res & [est-end]]
  (run-params (str s-res "-" t-res) est-end))

(defmain TimeseriesFilter
  [s-res t-res ts-path static-path output-path & {:keys [thrift] :or [thrift false]}]
  (let [vcf-limit (:vcf-limit (get-est-map s-res t-res))
        static-src (hfs-seqfile static-path)
        ts-src (hfs-seqfile ts-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (if (utils/arg-parser thrift)
      (?- sink (forma/filter-query static-src vcf-limit ts-src))
      (?- sink (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start ?series]
                   (ts-src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?series)
                   (static-src ?s-res ?mod-h ?mod-v ?sample ?line ?vcf _ _ _ _)
                   (>= ?vcf vcf-limit))))))

(defmain AdjustSeries
  [s-res t-res ndvi-path rain-path output-path]
  (let [est-map (get-est-map s-res t-res nil)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/dynamic-filter est-map
                                   (hfs-seqfile ndvi-path)
                                   (hfs-seqfile rain-path)))))

(defmain Trends
  [s-res t-res est-end adjusted-path output-path]
  (let [est-map (get-est-map s-res t-res est-end)
        adjusted-series-src (hfs-seqfile adjusted-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/analyze-trends est-map adjusted-series-src))))

(defmain FormaTap
  [s-res t-res est-end fire-path dynamic-path output-path]
  (let [est-map (get-est-map s-res t-res est-end)
        dynamic-src (hfs-seqfile dynamic-path)
        fire-src (hfs-seqfile fire-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/forma-tap est-map dynamic-src fire-src))))

(defmain NeighborQuery
  [s-res t-res forma-val-path output-path]
  (let [est-map (get-est-map s-res t-res nil)
        val-src (let [src (hfs-seqfile forma-val-path)]
                  (<- [?s-res ?period ?modh ?modv ?sample ?line ?forma-val]
                      (src ?s-res ?period ?modh ?modv ?sample ?line ?forma-val)))
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/neighbor-query est-map val-src))))

(defmain BetaDataPrep
  [s-res t-res dynamic-path static-path output-path]
  (let [est-map (get-est-map s-res t-res nil)
        dynamic-src (hfs-seqfile dynamic-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/beta-data-prep est-map dynamic-src static-src))))

(defmain GenBetas
  [s-res t-res est-start dynamic-path output-path]
  (let [est-map (get-est-map s-res t-res est-start)
        dynamic-src (hfs-seqfile dynamic-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/beta-gen est-map dynamic-src))))

(defmain EstimateForma
  [s-res t-res beta-path dynamic-path static-path output-path]
  (let [est-map (get-est-map s-res t-res nil)
        beta-src (hfs-seqfile beta-path)
        dynamic-src (hfs-seqfile dynamic-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/forma-estimate est-map beta-src dynamic-src static-src))))
