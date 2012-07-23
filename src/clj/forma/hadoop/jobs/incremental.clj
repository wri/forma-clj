(ns forma.hadoop.jobs.incremental
  (:use cascalog.api)
  (:require [forma.date-time :as date]
            [forma.ops.classify :as classify]
            [forma.trends.analysis :as analyze]            
            [forma.hadoop.jobs.scatter :as scatter]
            [forma.hadoop.jobs.forma :as forma]
            [cascalog.ops :as c]))

;; (defmain increment-prob
;;   [tmp-root pail-path ts-pail-path fire-pail-path out-path run-key]
;;   (let [{:keys [s-res t-res est-end] :as est-map} (scatter/forma-run-parameters run-key)
;;         betas (classify/beta-dict beta-src)]))

    ;; (date/datetime->period t-res end-period)

(defmain formarunner
  [tmp-root pail-path ts-pail-path fire-pail-path out-path run-key]
  (let [{:keys [s-res t-res est-end] :as est-map} (forma-run-parameters run-key)
        mk-filter (fn [vcf-path ts-src] (forma/filter-query (hfs-seqfile vcf-path)
                                                           (:vcf-limit est-map)
                                                           ts-src))]
    (assert est-map (str run-key " is not a valid run key!"))
 
    (workflow [tmp-root]

              ndvi-pail-seq-step
              ([:tmp-dirs ndvi-seq-path]
                 "Filters out NDVI with VCF < 25"
                 (?- (hfs-seqfile ndvi-seq-path)
                     (<- [?pail-path ?data-chunk]
                         ((constrained-tap ts-pail-path
                                           "ndvi"
                                           s-res
                                           t-res) ?pail-path ?data-chunk))))

              reli-pail-seq-step
              ([:tmp-dirs reli-seq-path]
                 "Filters out reliability with VCF < 25"
                 (?- (hfs-seqfile reli-seq-path)
                     (<- [?pail-path ?data-chunk]
                         ((constrained-tap ts-pail-path
                                           "reli"
                                           s-res
                                           t-res) ?pail-path ?data-chunk))))

              rain-pail-seq-step
              ([:tmp-dirs rain-seq-path]
                 "Filter out rain with VCF < 25"
                 (?- (hfs-seqfile rain-seq-path)
                     (<- [?pail-path ?data-chunk]
                         ((constrained-tap ts-pail-path
                                           "precl"
                                           s-res
                                           "32") ?pail-path ?data-chunk))))

              ndvi-filter
              ([:tmp-dirs ndvi-path]
                 "Filters out NDVI with VCF < 25"
                 (?- (hfs-seqfile ndvi-path) 
                     (mk-filter vcf-path (hfs-seqfile ndvi-seq-path))))

              reli-filter
              ([:tmp-dirs reli-path]
                 "Filters out reliability with VCF < 25"
                 (?- (hfs-seqfile reli-path)
                     (mk-filter vcf-path (hfs-seqfile reli-seq-path))))
              
              screen-rain
              ([:tmp-dirs rain-screened-path]
                 "Only keeps rain for a specific country"
                 (?- (hfs-seqfile rain-screened-path)
                     (forma/screen-by-tileset (hfs-seqfile rain-seq-path)
                                              (tile-set :IDN :BRA))))

              rain-filter
              ([:tmp-dirs rain-path]
                 "Filter out rain with VCF < 25"
                 (?- (hfs-seqfile rain-path)
                     (mk-filter vcf-path
                                (adjusted-precl-tap ts-pail-path
                                                    s-res
                                                    "32"
                                                    t-res
                                                    (hfs-seqfile rain-screened-path)))))
              trends
              ([:tmp-dirs trends-path]
                 "Runs the trends processing."
                 (?- (hfs-seqfile trends-path)
                     (forma/analyze-trends
                      est-map
                      (hfs-seqfile clean-series))))
              
              trends-cleanup
              ([:tmp-dirs cleanup-path]
                 "Clean up data after trends to improve join performance. Joins
                  kill us with lots of observations"
                 (?- (hfs-seqfile cleanup-path)
                     (forma/trends-cleanup (hfs-seqfile trends-path))))
              
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
                     (forma/forma-tap t-res
                                      est-map
                                      (hfs-seqfile cleanup-path)
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

              forma-estimate
              ([]
                 "Apply beta vector"
                 (?- (hfs-seqfile out-path :sinkmode :replace)
                     (forma/forma-estimate (hfs-seqfile beta-path)
                                           (hfs-seqfile final-path)
                                           (hfs-seqfile static-path))))

              stop-process
              ([]
                 "stop everything before deleting the temp directory"
                 (?- (hfs-seqfile "/mnt/hgfs/Dropbox/yikes")
                     (hfs-seqfile "/mnt/hgfs/Dropbox/yikestimes"))))))
