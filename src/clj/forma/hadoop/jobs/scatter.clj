(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (to-pail ?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)])
  (:require [cascalog.ops :as c]
            [forma.reproject :as r]
            [forma.schema :as schema]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]
            [forma.date-time :as date]
            [forma.classify.logistic :as log]
            [forma.thrift :as thrift]))

(defn forma-run-parameters [k est-end]
  (-> {"1000-32" {:est-start "2005-12-31"
                  :est-end est-end
                  :s-res "1000"
                  :t-res "32"
                  :neighbors 1
                  :window-dims [600 600]
                  :vcf-limit 25
                  :long-block 15
                  :window 5}
       "500-16" {:est-start "2005-12-31"
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

(defn pail-tap
  "Creates tap for a pail given the pail path, dataset name, and spatial
   and temporal resolution."
  [pail-path dataset s-res t-res]
  (split-chunk-tap pail-path [dataset (format "%s-%s" s-res t-res)]))

(defmain formarunner
  [tmp-root pail-path ts-pail-path fire-pail-path out-path run-key est-end]
  (let [{:keys [s-res t-res est-end] :as est-map} (forma-run-parameters run-key est-end)
        mk-filter (fn [static-path ts-src]
                    (forma/filter-query (hfs-seqfile static-path)
                                        (:vcf-limit est-map)
                                        ts-src))]
    (assert est-map (str run-key " is not a valid run key!"))

    (workflow [tmp-root]

              vcf-step
              ([:tmp-dirs vcf-path]
                 (?- (hfs-seqfile vcf-path)
                     (forma/static-tap (pail-tap pail-path "vcf" s-res "00"))))

              gadm-step
              ([:tmp-dirs gadm-path]
                 (?- (hfs-seqfile gadm-path)
                     (forma/static-tap (pail-tap pail-path "gadm" s-res "00"))))

              hansen-step
              ([:tmp-dirs hansen-path]
                 (?- (hfs-seqfile hansen-path)
                     (forma/static-tap (pail-tap pail-path "hansen" s-res "00"))))

              ecoid-step
              ([:tmp-dirs ecoid-path]
                 (?- (hfs-seqfile ecoid-path)
                     (forma/static-tap (pail-tap pail-path "ecoid" s-res "00"))))

              border-step
              ([:tmp-dirs border-path]
                 (?- (hfs-seqfile border-path)
                     (forma/static-tap (pail-tap pail-path "border" s-res "00"))))

              static-step
              ([:tmp-dirs static-path]
                 (?- (hfs-seqfile static-path)
                     (forma/consolidate-static (:vcf-limit est-map)
                                               (hfs-seqfile vcf-path)
                                               (hfs-seqfile gadm-path)
                                               (hfs-seqfile hansen-path)
                                               (hfs-seqfile ecoid-path)
                                               (hfs-seqfile border-path))))

              ndvi-pail-seq-step
              ([:tmp-dirs ndvi-seq-path]
                 "Convert ndvi pail to sequence files"
                 (?- (hfs-seqfile ndvi-seq-path)
                     (<- [?pail-path ?data-chunk]
                         ((pail-tap ts-pail-path
                                    "ndvi"
                                    s-res
                                    t-res) ?pail-path ?data-chunk))))

              rain-pail-seq-step
              ([:tmp-dirs rain-seq-path]
                 "Convert rain pail to sequence files"
                 (?- (hfs-seqfile rain-seq-path)
                     (<- [?pail-path ?data-chunk]
                         ((pail-tap ts-pail-path
                                    "precl"
                                    s-res
                                    "32") ?pail-path ?data-chunk))))

              ndvi-filter
              ([:tmp-dirs ndvi-path]
                 "Filters out NDVI with VCF < 25 or outside humid tropics"
                 (?- (hfs-seqfile ndvi-path)
                     (mk-filter static-path (hfs-seqfile ndvi-seq-path))))

              rain-filter
              ([:tmp-dirs rain-path]
                 "Filters out rain with VCF < 25 or outside humid tropics,
                  before stretching rain ts"
                 (let [rain-src (hfs-seqfile rain-seq-path)
                       static-src (hfs-seqfile static-path)
                       vcf-limit (est-map :vcf-limit)]
                   (?<- (hfs-seqfile rain-path)
                        [?mod-h ?mod-v ?sample ?line ?start ?series]
                        (rain-src ?mod-h ?mod-v ?sample ?line ?start ?series)
                        (static-src _ ?mod-h ?mod-v ?sample ?line ?vcf _ _ _ _)
                        (>= ?vcf vcf-limit))))

              adjustseries
              ([:tmp-dirs adjusted-series-path]
                 "Adjusts lengths of all timeseries so they all cover the same
                  time spans."
                 (with-job-conf {"mapred.min.split.size" 805306368}
                   (?- (hfs-seqfile adjusted-series-path)
                       (forma/dynamic-filter est-map
                                             (hfs-seqfile ndvi-path)
                                             (hfs-seqfile rain-path)))))

              trends
              ([:tmp-dirs trends-path]
                 "Runs the trends processing."
                 (?- (hfs-seqfile trends-path)
                     (forma/analyze-trends
                      est-map
                      (hfs-seqfile adjusted-series-path))))

              fire-step
              ([:tmp-dirs fire-path]
                 "Create fire series"
                 (?- (hfs-seqfile fire-path)
                     (tseries/fire-query fire-pail-path
                                         s-res
                                         t-res
                                         "2000-11-01"
                                         est-end)))

              adjustfires
              ([:tmp-dirs adjusted-fire-path]
                 "Make sure fires data lines up temporally with our other
                  timeseries."
                 (?- (hfs-seqfile adjusted-fire-path)
                     (forma/fire-tap est-map (hfs-seqfile fire-path))))

              mid-forma
              ([:tmp-dirs forma-mid-path]
                 "Final step to collect all data for the feature vector -
                 trends + fires data"
                 (?- (hfs-seqfile forma-mid-path)
                     (forma/forma-tap est-map
                                      (hfs-seqfile trends-path)
                                      (hfs-seqfile adjusted-fire-path))))

              final-forma
              ([:tmp-dirs final-path]
                 "Process neighbors"
                 (let [names ["?s-res" "?period" "?mod-h" "?mod-v"
                              "?sample" "?line" "?forma-val"]
                       mid-src (-> (hfs-seqfile forma-mid-path)
                                   (name-vars names))]
                   (?- (hfs-seqfile final-path)
                       (forma/forma-query est-map mid-src))))

              beta-data-prep
              ([:tmp-dirs beta-data-path]
                 "Pre-generate data needed for beta generation. Saves a bit of
                  time if you have to run the next step multiple times."
                 (?- (hfs-seqfile beta-data-path)
                     (forma/beta-data-prep est-map
                                           (hfs-seqfile final-path)
                                           (hfs-seqfile static-path))))

              gen-betas
              ([:tmp-dirs beta-path]
                 "Generate beta vector"
                 (with-job-conf
                   {"mapred.reduce.tasks" 200
                    "mapred.child.java.opts" "-Xmx2048M"}
                   (?- (hfs-seqfile beta-path)
                       (forma/beta-gen est-map (hfs-seqfile beta-data-path)))))

              forma-estimate
              ([]
                 "Apply beta vector"
                 (?- (hfs-seqfile out-path :sinkmode :replace)
                     (forma/forma-estimate est-map
                                           (hfs-seqfile beta-path)
                                           (hfs-seqfile final-path)
                                           (hfs-seqfile static-path))))

              stop-process
              ([]
                 "stop everything before deleting the temp directory"
                 (?- (hfs-seqfile "/mnt/hgfs/Dropbox/yikes")
                     (hfs-seqfile "/mnt/hgfs/Dropbox/yikestimes"))))))

(defmain simplerunner
  [tmp-root pail-path ts-pail-path out-path run-key est-end]
  (let [{:keys [s-res t-res est-end] :as est-map} (forma-run-parameters run-key est-end)
        mk-filter (fn [vcf-path ts-src]
                    (forma/filter-query (hfs-seqfile vcf-path)
                                        (:vcf-limit est-map)
                                        ts-src))]
    (assert est-map (str run-key " is not a valid run key!"))
    (workflow [tmp-root]
              vcf-step
              ([:tmp-dirs vcf-path]
                 (?- (hfs-seqfile vcf-path)
                     (<- [?subpail ?data-chunk]
                         ((pail-tap pail-path "vcf" s-res "00") ?subpail ?data-chunk))))


              ndvi-step
              ([:tmp-dirs ndvi-path]
                 (?- (hfs-seqfile ndvi-path)
                     (mk-filter vcf-path
                                (pail-tap
                                 ts-pail-path "ndvi" s-res t-res)))))))
