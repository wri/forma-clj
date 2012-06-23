(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (to-pail ?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)]
        [clojure.math.numeric-tower :only (round)])
  (:require [cascalog.ops :as c]
            [forma.utils :only (throw-illegal)]
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
;;      (p/index ?vals :> ?pixel-idx ?val)
      (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
;;      (r/tile-position ?s-res ?size ?id ?pixel-idx :> ?sample ?line)

      ;;(p/blossom-chunk ?chunk :> ?s-res ?mod-h ?mod-v ?sample ?line
      ;;?val)
      ))

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
   "500-16" {:est-start "2005-12-31"
             :est-end "2012-01-17"
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
             :min-coast-dist 3}})

(defn constrained-tap
  [ts-pail-path dataset s-res t-res]
  (split-chunk-tap ts-pail-path [dataset (format "%s-%s" s-res t-res)]))

(defn map-round
  [series-obj]
  (let [[start end series] (thrift/unpack series-obj)]
    (thrift/TimeSeries* (long start) (long end) (map round (thrift/unpack series)))))

(defn adjusted-precl-tap
  "Document... returns a tap that adjusts for the incoming
  resolution."
  [ts-pail-path s-res base-t-res t-res]
  (let [precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res)]
    (if (= t-res base-t-res)
      precl-tap
      (<- [?path ?adjusted-pixel-chunk]
          (precl-tap ?path ?pixel-chunk)
          (thrift/unpack ?pixel-chunk :> ?name ?in-pix-loc ?ts ?t-res _)
          (thrift/unpack ?in-pix-loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
          (stretch/ts-expander base-t-res t-res ?ts :> ?expanded-ts)
          (map-round ?expanded-ts :> ?rounded-ts)
          (thrift/ModisPixelLocation* ?s-res ?mod-h ?mod-v ?sample ?line :> ?out-pix-loc)
          (thrift/DataChunk* ?name ?out-pix-loc ?rounded-ts ?t-res :> ?adjusted-pixel-chunk)
          (:distinct false)))))

(defmain formarunner
  [tmp-root pail-path ts-pail-path out-path run-key]
  (let [{:keys [s-res t-res est-end] :as est-map} (forma-run-parameters run-key)
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
                                 ts-pail-path "ndvi" s-res t-res))))

              reli-step
              ([:tmp-dirs reli-path]
                 (?- (hfs-seqfile reli-path)
                     (mk-filter vcf-path
                                (constrained-tap
                                 ts-pail-path "reli" s-res t-res))))

              rain-step
              ([:tmp-dirs rain-path]
                 (?- (hfs-seqfile rain-path)
                     (mk-filter vcf-path
                                (adjusted-precl-tap
                                 ts-pail-path s-res "32" t-res))))
              
              adjustseries
              ([:tmp-dirs adjusted-series-path]
                 "Adjusts lengths of all timeseries so they all cover the same
                  time spans."
                 (with-job-conf {"mapred.min.split.size" 805306368}
                   (?- (hfs-seqfile adjusted-series-path)
                       (forma/dynamic-filter (hfs-seqfile ndvi-path)
                                             (hfs-seqfile reli-path)
                                             (hfs-seqfile rain-path)))))

              fire-step
              ([:tmp-dirs fire-path]
                 (?- (hfs-seqfile fire-path)
                     (tseries/fire-query pail-path
                                         t-res
                                         "2000-11-01"
                                         est-end)))

              adjustfires
              ([:tmp-dirs adjusted-fire-path]
                 (?- (hfs-seqfile adjusted-fire-path)
                     (forma/fire-tap est-map (hfs-seqfile fire-path))))

              cleanseries
              ([:tmp-dirs clean-series]
                 "Runs the trends processing."
                 (?- (hfs-seqfile clean-series)
                     (forma/dynamic-clean
                      est-map (hfs-seqfile adjusted-series-path))))

              trends
              ([:tmp-dirs trends-path]
                 "Runs the trends processing."
                 (?- (hfs-seqfile trends-path)
                     (forma/analyze-trends
                      est-map (hfs-seqfile clean-series))))
              
              trends-cleanup
              ([:tmp-dirs cleanup-path]
                 "Clean up data after trends to improve join performance"
                 (?- (hfs-seqfile cleanup-path)
                     (forma/trends-cleanup (hfs-seqfile trends-path))))

              mid-forma
              ([:tmp-dirs forma-mid-path
                :deps [trends adjustfires]]
                 (?- (hfs-seqfile forma-mid-path)
                     (forma/forma-tap (hfs-seqfile trends-path)
                                      (hfs-seqfile adjusted-fire-path))))
              
              final-forma
              ([:tmp-dirs final-path]
                 (let [names ["?s-res" "?period" "?mod-h" "?mod-v"
                              "?sample" "?line" "?forma-val"]
                       mid-src (-> (hfs-seqfile forma-mid-path)
                                   (name-vars names))]
                   (?- (hfs-seqfile final-path)
                       (forma/forma-query est-map mid-src))))

              beta-data-prep
              ([:tmp-dirs beta-data-path]
                 (?- (hfs-seqfile beta-data-path)
                     (forma/beta-data-prep est-map
                                           (hfs-seqfile final-path)
                                           (static-src est-map pail-path))))

              gen-betas
              ([:tmp-dirs beta-path]
                 (?- (hfs-seqfile beta-path)
                       (forma/beta-gen est-map (hfs-seqfile beta-data-path))))

              forma-estimate
              ([] (?- (hfs-seqfile out-path :sinkmode :replace)
                      (forma/forma-estimate (hfs-seqfile beta-path)
                                            (hfs-seqfile final-path)
                                            (static-src est-map pail-path)))))))

