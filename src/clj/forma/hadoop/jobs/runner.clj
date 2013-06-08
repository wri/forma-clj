(ns forma.hadoop.jobs.runner
  "This namespace includes defmains needed for running the various
   steps in the FORMA workflow. They can be used from the REPL or
   shell, and arguments are parsed appropriately. See tests for usage
   examples."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set country-tiles)])
  (:require [forma.hadoop.jobs.forma :as forma]
            [forma.utils :as utils]
            [forma.thrift :as thrift]
            [forma.hadoop.pail :as p]))

(defn run-params [k est-start est-end]
  (-> {"500-16" {:est-start (or est-start "2005-12-19")
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
  [s-res t-res & {:keys [est-start est-end] :or {:est-start nil :est-end nil}}]
  (run-params (str s-res "-" t-res) est-start est-end))

(defn parse-pedigree
  "Parse with various forms of pedigree - nils, strings, and ints."
  [pedigree]
  (if (or (nil? pedigree)
          (empty? pedigree)) ;; empty string
    (thrift/epoch)
    (if (string? pedigree)
      (read-string pedigree)
      pedigree)))

(defmain TimeseriesFilter
  "Uses thrift-bool as switch instead of {:keys [thrift] ...} because use
   with defmain seemed unreliable - keyword were not recognized as such."
  [s-res t-res ts-path static-path output-path]
  (let [vcf-limit (:vcf-limit (get-est-map s-res t-res))
        static-src (hfs-seqfile static-path)
        ts-src (hfs-seqfile ts-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/filter-query static-src vcf-limit ts-src))))

(defmain AdjustSeries
  [s-res t-res ndvi-path rain-path output-path]
  (let [est-map (get-est-map s-res t-res)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/dynamic-filter est-map
                                   (hfs-seqfile ndvi-path)
                                   (hfs-seqfile rain-path)))))

(defmain Trends
  [s-res t-res est-end adjusted-path output-path & [est-start]]
  (let [est-map (get-est-map s-res t-res :est-end est-end :est-start est-start)
        adjusted-series-src (hfs-seqfile adjusted-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/analyze-trends est-map adjusted-series-src))))

(defmain TrendsPail
  [s-res t-res est-end trends-path output-path & pedigree] ;; pedigree helps w/testing
  (let [pedigree (parse-pedigree pedigree)
        est-map (get-est-map s-res t-res :est-end est-end)
        trends-src (hfs-seqfile trends-path)]
    (p/to-pail output-path
               (forma/trends->datachunks est-map trends-src))))

(defmain MergeTrends
  [s-res t-res est-end trends-pail-path output-path]
  (let [est-map (get-est-map s-res t-res :est-end est-end)
        trends-dc-src (p/split-chunk-tap trends-pail-path ["trends" (format "%s-%s" s-res t-res)])
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/trends-datachunks->series est-map trends-dc-src))))

(defmain FormaTap
  [s-res t-res est-start est-end fire-path dynamic-path output-path]
  (let [est-map (get-est-map s-res t-res :est-start est-start :est-end est-end)
        dynamic-src (hfs-seqfile dynamic-path)
        fire-src (hfs-seqfile fire-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/forma-tap est-map dynamic-src fire-src))))

(defmain NeighborQuery
  [s-res t-res forma-val-path output-path]
  (let [est-map (get-est-map s-res t-res)
        val-src (let [src (hfs-seqfile forma-val-path)]
                  (<- [?s-res ?period ?modh ?modv ?sample ?line ?forma-val]
                      (src ?s-res ?period ?modh ?modv ?sample ?line ?forma-val)))
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/neighbor-query est-map val-src))))

(defmain BetaDataPrep
  [s-res t-res dynamic-path static-path output-path]
  (let [est-map (get-est-map s-res t-res)
        dynamic-src (hfs-seqfile dynamic-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/beta-data-prep est-map dynamic-src static-src))))

(defmain GenBetas
  [s-res t-res est-start dynamic-path output-path]
  (let [est-map (get-est-map s-res t-res :est-start est-start)
        dynamic-src (hfs-seqfile dynamic-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/beta-gen est-map dynamic-src))))

(defmain EstimateForma
  [s-res t-res beta-path dynamic-path static-path output-path]
  (let [est-map (get-est-map s-res t-res)
        beta-src (hfs-seqfile beta-path)
        dynamic-src (hfs-seqfile dynamic-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/forma-estimate est-map beta-src dynamic-src static-src))))

(defmain ProbsPail
  [s-res t-res est-end probs-path output-path & pedigree] ;; pedigree helps w/testing
  (let [pedigree (parse-pedigree pedigree)
        est-map (get-est-map s-res t-res :est-end est-end)
        probs-src (hfs-seqfile probs-path)]
    (p/to-pail output-path
               (forma/probs->datachunks est-map probs-src))))

(defmain MergeProbs
  [s-res t-res est-end probs-pail-path output-path]
  (let [data-name "forma"
        est-map (get-est-map s-res t-res :est-end est-end)
        probs-dc-src (p/split-chunk-tap probs-pail-path [data-name (format "%s-%s" s-res t-res)])
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/probs-datachunks->series est-map probs-dc-src))))
