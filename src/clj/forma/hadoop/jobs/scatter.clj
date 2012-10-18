(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (to-pail ?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)]
        [clojure.math.numeric-tower :only (round)])
  (:require [cascalog.ops :as c]
            [forma.reproject :as r]
            [forma.schema :as schema]
            [forma.trends.stretch :as stretch]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]
            [forma.date-time :as date]
            [forma.classify.logistic :as log]
            [forma.thrift :as thrift]))

(def convert-line-src
  (hfs-textline "s3n://modisfiles/ascii/admin-map.csv"))

(defn static-tap
  "Accepts a source of DataChunks containing vectors as values, and
  returns a new query with all relevant spatial information plus the
  actual value."
  [chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?val]
      (chunk-src _ ?chunk)
      (thrift/unpack ?chunk :> _ ?loc ?data _ _)
      (thrift/get-field-value ?data :> ?val)
      (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)))

(defn country-tap
  [gadm-src convert-src]
  (let [gadm-tap (static-tap gadm-src)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?country]
        (gadm-tap ?s-res ?mod-h ?mod-v ?sample ?line ?admin)
        (convert-src ?textline)
        (p/converter ?textline :> ?country ?admin))))

(defmain GetStatic
  [pail-path out-path]
  (let [[vcf hansen ecoid gadm border]
        (map (comp static-tap (partial split-chunk-tap pail-path))
             [["vcf"] ["hansen"] ["ecoid"] ["gadm"] ["border"]])]
    (?<- (hfs-textline out-path
                       :sinkparts 3
                       :sink-template "%s/")
         [?country ?lat ?lon ?mod-h ?mod-v
          ?sample ?line ?hansen ?ecoid ?vcf ?gadm ?border]
         (vcf    ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
         (hansen ?s-res ?mod-h ?mod-v ?sample ?line ?hansen)
         (ecoid  ?s-res ?mod-h ?mod-v ?sample ?line ?ecoid)
         (gadm   ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
         (border ?s-res ?mod-h ?mod-v ?sample ?line ?border)
         (r/modis->latlon ?s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
         (convert-line-src ?textline)
         (p/converter ?textline :> ?country ?gadm)
         (>= ?vcf 25))))

(defn static-src [{:keys [vcf-limit]} pail-path]
  ;; screen out border pixels later - doing it here will remove non-
  ;; but nearly water pixels before they can be included as neighbors
  (let [[vcf hansen ecoid gadm border]
        (map (comp static-tap (partial split-chunk-tap pail-path))
             [["vcf"] ["hansen"] ["ecoid"] ["gadm"] ["border"]])]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?gadm ?vcf ?ecoid ?hansen ?coast-dist]
        (vcf    ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
        (hansen ?s-res ?mod-h ?mod-v ?sample ?line ?hansen)
        (ecoid  ?s-res ?mod-h ?mod-v ?sample ?line ?ecoid)
        (gadm   ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
        (border ?s-res ?mod-h ?mod-v ?sample ?line ?coast-dist)
        (>= ?vcf vcf-limit))))

;; ## Forma

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
                 :nodata -9999}}
      (get k)))

(defn constrained-tap
  [ts-pail-path dataset s-res t-res]
  (split-chunk-tap ts-pail-path [dataset (format "%s-%s" s-res t-res)]))

(defn map-round
  "Round the values of a timeseries in a TimeSeries object, returning
   the starting period and the rounded timeseries."
  [series-obj]
  (let [[start _ series] (thrift/unpack series-obj)]
    [start (vec (map round (thrift/unpack series)))]))

(defn adjusted-precl-tap
  "Document... returns a tap that adjusts for the incoming
  resolution."
  [s-res base-t-res t-res src]
  (if (= t-res base-t-res)
      src ;;(constrained-tap ts-path "precl" s-res base-t-res)
      (<- [?s-res ?mod-h ?mod-v ?sample ?line ?new-start-idx ?rounded-ts]
          (src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?ts)
          (thrift/TimeSeries* ?start-idx ?ts :> ?ts-obj)
          (stretch/ts-expander base-t-res t-res ?ts-obj :> ?expanded-ts)
          (map-round ?expanded-ts :> ?new-start-idx ?rounded-ts)
          (:distinct false))))

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
                     (static-tap (constrained-tap pail-path "vcf" s-res "00"))))

              gadm-step
              ([:tmp-dirs gadm-path]
                 (?- (hfs-seqfile gadm-path)
                     (static-tap (constrained-tap pail-path "gadm" s-res "00"))))

              hansen-step
              ([:tmp-dirs hansen-path]
                 (?- (hfs-seqfile hansen-path)
                     (static-tap (constrained-tap pail-path "hansen" s-res "00"))))

              ecoid-step
              ([:tmp-dirs ecoid-path]
                 (?- (hfs-seqfile ecoid-path)
                     (static-tap (constrained-tap pail-path "ecoid" s-res "00"))))

              border-step
              ([:tmp-dirs border-path]
                 (?- (hfs-seqfile border-path)
                     (static-tap (constrained-tap pail-path "border" s-res "00"))))

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
                         ((constrained-tap ts-pail-path
                                           "ndvi"
                                           s-res
                                           t-res) ?pail-path ?data-chunk))))

              reli-pail-seq-step
              ([:tmp-dirs reli-seq-path]
                 "Convert reliability pail to sequence files"
                 (?- (hfs-seqfile reli-seq-path)
                     (<- [?pail-path ?data-chunk]
                         ((constrained-tap ts-pail-path
                                           "reli"
                                           s-res
                                           t-res) ?pail-path ?data-chunk))))

              rain-pail-seq-step
              ([:tmp-dirs rain-seq-path]
                 "Convert rain pail to sequence files"
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
                     (mk-filter static-path (hfs-seqfile ndvi-seq-path))))

              reli-filter
              ([:tmp-dirs reli-path]
                 "Filters out reliability with VCF < 25"
                 (?- (hfs-seqfile reli-path)
                     (mk-filter static-path (hfs-seqfile reli-seq-path))))

              rain-filter
              ([:tmp-dirs rain-path]
                 "Filters out rain with VCF < 25, before stretching rain ts"
                 (?- (hfs-seqfile rain-path)
                     (mk-filter static-path (hfs-seqfile rain-seq-path))))

              rain-stretcher
              ([:tmp-dirs rain-stretch-path]
                 "Stretch rain timeseries to match MODIS timeseries resolution"
                 (?- (hfs-seqfile rain-stretch-path)
                     (adjusted-precl-tap s-res "32" t-res (hfs-seqfile rain-path))))

              adjustseries
              ([:tmp-dirs adjusted-series-path]
                 "Adjusts lengths of all timeseries so they all cover the same
                  time spans."
                 (with-job-conf {"mapred.min.split.size" 805306368}
                   (?- (hfs-seqfile adjusted-series-path)
                       (forma/dynamic-filter (hfs-seqfile ndvi-path)
                                             (hfs-seqfile reli-path)
                                             (hfs-seqfile rain-stretch-path)))))

              cleanseries
              ([:tmp-dirs clean-series]
                 "Screen out extremely cloudy pixels. Currently doesn't
                  actually run the timeseries cleaning function"
                 (?- (hfs-seqfile clean-series)
                     (forma/dynamic-clean
                      est-map
                      (hfs-seqfile adjusted-series-path))))

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
                     (forma/forma-estimate (hfs-seqfile beta-path)
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
                         ((constrained-tap pail-path "vcf" s-res "00") ?subpail ?data-chunk))))


              ndvi-step
              ([:tmp-dirs ndvi-path]
                 (?- (hfs-seqfile ndvi-path)
                     (mk-filter vcf-path
                                (constrained-tap
                                 ts-pail-path "ndvi" s-res t-res)))))))
