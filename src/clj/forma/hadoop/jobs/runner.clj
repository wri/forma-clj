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
            [forma.hadoop.pail :as p]
            [forma.hadoop.jobs.api :as api]
            [forma.hadoop.jobs.postprocess :as postprocess]))

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
                 :nodata -9999.0
                 :discount-map {60122 0.51 60147 0.51 30109 0.51 40134 0.51 40150 0.51
                                40131 0.51 40136 0.51 40126 0.51 30130 0.51 40141 0.51
                                40132 0.51 40120 0.51 40135 0.51 30123 0.51}}}
      (get k)))

(def api-config
  {:long {:query api/prob-series->long-ts
          :template-fields ["?iso-extra" "?year"]
          :sink-template "%s/%s"}
   :first {:query api/prob-series->first-hit
           :template-fields ["?iso-extra"]
           :sink-template "%s/"}
   :latest {:query api/prob-series->latest
            :template-fields ["?iso-extra"]
            :sink-template "%s/"}})

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
  [s-res t-res dynamic-path static-path output-path super-ecoregions]
  (let [super-ecoregions (if (string? super-ecoregions)
                           (read-string super-ecoregions)
                           super-ecoregions)
        est-map (get-est-map s-res t-res)
        dynamic-src (hfs-seqfile dynamic-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/beta-data-prep est-map dynamic-src static-src
                                   :super-ecoregions super-ecoregions))))

(defmain GenBetas
  [s-res t-res est-start dynamic-path output-path]
  (let [est-map (get-est-map s-res t-res :est-start est-start)
        dynamic-src (hfs-seqfile dynamic-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/beta-gen est-map dynamic-src))))

(defmain EstimateForma
  [s-res t-res beta-path dynamic-path static-path output-path super-ecoregions]
  (let [super-ecoregions (if (string? super-ecoregions)
                           (read-string super-ecoregions)
                           super-ecoregions)
        est-map (get-est-map s-res t-res)
        beta-src (hfs-seqfile beta-path)
        dynamic-src (hfs-seqfile dynamic-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/forma-estimate est-map beta-src dynamic-src static-src
                                   :super-ecoregions super-ecoregions))))

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

(defn get-sink-template
  [api-kw api-config]
  (:sink-template (api-kw api-config)))

(defn get-template-fields
  [api-kw api-config]
  (:template-fields (api-kw api-config)))

(defn get-query
  [api-kw api-config]
  (:query (api-kw api-config)))

(defn get-output-path
  [api-str thresh base-path]
  (format "%s/%s/pantropical/%s" base-path api-str thresh))

(defmain ApiRunner
  [api-str thresh s-res t-res forma-gadm2-path base-output-path]
  {:pre [(contains? (set [:long :first :latest]) (keyword api-str))]}
  (let [api-kw (keyword api-str)
        est-map (get-est-map s-res t-res)
        out-fields ["?lat" "?lon" "?iso" "?gadm2" "?date" "?prob"]
        src (hfs-seqfile forma-gadm2-path)
        sink-template (get-sink-template api-kw api-config)
        template-fields (get-template-fields api-kw api-config)
        query (get-query api-kw api-config)
        out-path (get-output-path api-str thresh base-output-path)
        sink (hfs-seqfile out-path :sinkmode :replace)]
    (?- sink (query est-map src :thresh thresh :pantropical true))))

(defmain ProbsGadm2
  [probs-path gadm2-path static-path output-path]
  (let [probs-src (hfs-seqfile probs-path)
        gadm-src (hfs-seqfile gadm2-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-seqfile output-path :sinkmode :replace)]
    (?- sink (forma/probs-gadm2 probs-src gadm-src static-src))))

(defmain Cdm
  "Convert output to common data model for use on GFW website.

   Sample parameters:
     thresh: 50
     z: 17
     nodata: -9999.0
     t-res: \"16\"
     out-t-res: \"32\"
     est-start: \"2005-12-19\"
     thresh: 50"
  [thresh z s-res t-res out-t-res est-start nodata src-path output-path]
  (let [z (Integer/parseInt z)
        thresh (Integer/parseInt thresh)
        nodata (Float/parseFloat nodata)
        disc-map (:discount-map (get-est-map s-res t-res))
        src (hfs-seqfile src-path)
        sink (hfs-textline output-path :sinkmode :replace)]
    (?- sink (postprocess/forma->cdm src nodata z t-res out-t-res est-start
                                     thresh disc-map))))

(defmain BlueRaster
  [s-res t-res nodata src-path static-path output-path]
  (let [nodata (Float/parseFloat nodata)
        disc-map (:discount-map (get-est-map s-res t-res))
        src (hfs-seqfile src-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-textline output-path :sinkmode :replace)]
    (?- sink (postprocess/forma->blue-raster src static-src nodata disc-map))))

(defmain FormaDownload
  [thresh s-res t-res nodata src-path output-path]
  (let [disc-map (:discount-map (get-est-map s-res t-res))
        thresh (Integer/parseInt thresh)
        nodata (Float/parseFloat nodata)
        src (hfs-seqfile src-path)
        sink (hfs-textline output-path :sinkmode :replace)]
    (?- sink (postprocess/forma-download src thresh t-res nodata disc-map))))
