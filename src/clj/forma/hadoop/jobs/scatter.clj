(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        forma.trends.data
        [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)])
  (:require [cascalog.ops :as c]
            [forma.utils :only (throw-illegal)]
            [forma.reproject :as r]
            [forma.schema :as schema]
            [forma.trends.stretch :as stretch]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]
            [forma.date-time :as date]
            [forma.classify.logistic :as log]))

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
  (let [[vcf hansen ecoid gadm border]
        (map (comp static-tap (partial split-chunk-tap pail-path))
             [["vcf"] ["hansen"] ["ecoid"] ["gadm"] ["border"]])]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?gadm ?vcf ?ecoid ?hansen]
        (vcf    ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
        (hansen ?s-res ?mod-h ?mod-v ?sample ?line ?hansen)
        (ecoid  ?s-res ?mod-h ?mod-v ?sample ?line ?ecoid)
        (gadm   ?s-res ?mod-h ?mod-v ?sample ?line ?gadm)
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
        (schema/unpack-pixel-location ?locat)
        (stretch/ts-expander base-t-res t-res ?series :> ?new-series)
        (assoc ?chunk
          :value ?new-series
          :temporal-res t-res :> ?final-chunk)
        (:distinct false)))))

(defmapcatop expand-rain-pixel
  [sample line]
  (let [sample-0 (* 2 sample)
        line-0   (* 2 line)]
    (for [x (range 2)
          y (range 2)]
      [(+ sample-0 x) (+ line-0 y)])))

(defn new-adjusted-precl-tap
  "More brittle version that assumes conversion from 1000 to 500m."
  [ts-pail-path s-res base-t-res t-res country-seq]
  (let [precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res country-seq)]
    (<- [?path ?final-chunk]
        (precl-tap ?path ?chunk)
        (forma/get-loc ?chunk :> ?s-res ?mod-h ?mod-v ?s ?l ?series)
        (expand-rain-pixel ?s ?l :> ?sample ?line)
        (schema/pixel-location "500" ?mod-h ?mod-v ?sample ?line :> ?location)
        (stretch/ts-expander base-t-res t-res ?series :> ?new-series)
        (assoc ?chunk
          :value ?new-series
          :location ?location
          :temporal-res t-res :> ?final-chunk)
        (:distinct false))))

(comment
  (??<- [?a ?b]
        ((constrained-tap
          "/Users/sritchie/Desktop/mypail" "vcf" "500" "00" [[8 6]]) ?a ?b)
        (:distinct false))

  "This command runs FORMA."
  (use 'forma.hadoop.jobs.scatter)
  (in-ns 'forma.hadoop.jobs.scatter)
  (formarunner "/user/hadoop/checkpoints"
               "s3n://pailbucket/master"
               "s3n://pailbucket/series"
               "s3n://formaresults/forma2012"
               "500-16"
               [:IDN]))

(defn first-half-query
  "Poorly named! Returns a query that generates a number of position
  and dataset identifier"
  [pail-path ts-pail-path out-path run-key country-seq]
  (let [{:keys [s-res t-res est-end] :as forma-map} (forma-run-parameters run-key)]
    (assert forma-map (str run-key " is not a valid run key!"))
    (with-job-conf {"mapreduce.jobtracker.split.metainfo.maxsize" 100000000000}
      (?- (hfs-seqfile out-path :sinkmode :replace)
          (forma/forma-query
           forma-map
           (constrained-tap ts-pail-path "ndvi" s-res t-res country-seq)
           (constrained-tap ts-pail-path "reli" s-res t-res country-seq)
           (new-adjusted-precl-tap ts-pail-path "1000" "32" t-res country-seq)
           (constrained-tap pail-path "vcf" s-res "00" country-seq)
           (tseries/fire-query pail-path
                               t-res
                               "2000-11-01"
                               est-end
                               country-seq))))))

(defmain formarunner
  [tmp-root pail-path ts-pail-path out-path run-key country-seq]
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
                     (<- [?a ?b]
                         ((constrained-tap
                           pail-path "vcf" s-res "00" country-seq) ?a ?b)
                         (:distinct false))))

              ndvi-step
              ([:tmp-dirs ndvi-path]
                 (with-job-conf {"cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                   (?- (hfs-seqfile ndvi-path)
                       (mk-filter vcf-path
                                  (constrained-tap
                                   ts-pail-path "ndvi" s-res t-res country-seq)))))

              fire-step ([:tmp-dirs fire-path]
                           (?- (hfs-seqfile fire-path)
                               (tseries/fire-query pail-path
                                                   t-res
                                                   "2000-11-01"
                                                   est-end
                                                   country-seq)))

              adjustfires
              ([:tmp-dirs adjusted-fire-path]
                 (with-job-conf
                   {"cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                   (?- (hfs-seqfile adjusted-fire-path)
                       (forma/fire-tap est-map (hfs-seqfile fire-path)))))

              rain-step
              ([:tmp-dirs rain-path]
                 (with-job-conf {"cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                   (?- (hfs-seqfile rain-path)
                       (mk-filter vcf-path
                                  (new-adjusted-precl-tap
                                   ts-pail-path "1000" "32" t-res country-seq)))))

              reli-step
              ([:tmp-dirs reli-path]
                 (with-job-conf {"cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                   (?- (hfs-seqfile reli-path)
                       (mk-filter vcf-path
                                  (constrained-tap
                                   ts-pail-path "reli" s-res t-res country-seq)))))
              
              adjustseries
              ([:tmp-dirs adjusted-series-path]
                 "Adjusts the lengths of all timeseries
                               and filters out timeseries below the proper VCF value."
                 (with-job-conf {"mapred.min.split.size" 805306368
                                 "cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                   (?- (hfs-seqfile adjusted-series-path)
                       (forma/dynamic-filter (hfs-seqfile ndvi-path)
                                             (hfs-seqfile reli-path)
                                             (hfs-seqfile rain-path)))))

              trends ([:tmp-dirs dynamic-path]
                        "Runs the trends processing."
                        (with-job-conf {"cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                          (?- (hfs-seqfile dynamic-path)
                              (forma/dynamic-tap
                               est-map (hfs-seqfile adjusted-series-path)))))

              mid-forma ([:tmp-dirs forma-mid-path
                          :deps [trends adjustfires]]
                           (with-job-conf {"cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                             (?- (hfs-seqfile forma-mid-path)
                                 (forma/forma-tap (hfs-seqfile dynamic-path)
                                                  (hfs-seqfile adjusted-fire-path)))))
              
              final-forma
              ([] (let [names ["?s-res" "?period" "?mod-h" "?mod-v"
                               "?sample" "?line" "?forma-val"]
                        mid-src (-> (hfs-seqfile forma-mid-path)
                                    (name-vars names))]
                    (with-job-conf {"cascading.kryo.serializations" "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"}
                      (?- (hfs-seqfile out-path)
                          (forma/forma-query est-map mid-src))))))))

(def kryo-conf
  {"cascading.kryo.serializations"   "forma.schema.TimeSeriesValue,carbonite.PrintDupSerializer:forma.schema.FireValue,carbonite.PrintDupSerializer:forma.schema.FormaValue,carbonite.PrintDupSerializer:forma.schema.NeighborValue,carbonite.PrintDupSerializer"
   "fs.s3n.multipart.uploads.enabled" true})

(defmain ultrarunner
  [tmp-root eco-beta-path full-beta-path static-path final-path out-path country-or-eco]
  (let [est-map (forma-run-parameters "500-16")]
    (workflow [tmp-root]              
              genbetas
              ([]
                 (let [beta-path (if (= "eco" country-or-eco)
                                   eco-beta-path
                                   full-beta-path)]
                   (?- (hfs-seqfile beta-path)
                       (forma/beta-generator est-map
                                             (hfs-seqfile final-path)
                                             (hfs-seqfile static-path))))))))
              ;; applybetas
              ;; ([] (?- (hfs-seqfile out-path)
              ;;         (forma/forma-estimate (hfs-seqfile eco-beta-path)
              ;;                               (hfs-seqfile final-path)
              ;;                               (hfs-seqfile static-path))))

;; (comment
;;   "Run this:"
;;   (ultrarunner "/user/hadoop/checkpoint"
;;                "s3n://formaresults/ecobetatemp"
;;                "s3n://formaresults/fullbetatemp"               
;;                "s3n://formaresults/staticbuckettemp"
;;                "s3n://formaresults/finalbuckettemp"
;;                "s3n://formaresults/finaloutput"
;;                "eco"))



(defbufferop eco-bufferop
  "returns a vector of parameter coefficients.  note that this is
  where the intercept is added (to the front of each stacked vector in
  the feature matrix

  TODO: The intercept is included in the feature vector for now, as a
  kludge when we removed the hansen statistic.  When we include the
  hansen stat, we will have to replace the feature-mat binding below
  with a line that tacks on a 1 to each feature vector.
  "
  [tuples]
  (let [val-mat      (map second tuples) 
        neighbor-mat (map last tuples)
        feature-mat  (map log/unpack-feature-vec val-mat neighbor-mat)]
    [[]]))

(defn sink-eco
  [{:keys [t-res est-start ridge-const convergence-thresh max-iterations]}
   dynamic-src static-src ecoid]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?hansen ?val ?neighbor-val]
        (dynamic-src ?s-res ?pd ?mod-h ?mod-v ?s ?l ?val ?neighbor-val)
        (static-src ?s-res ?mod-h ?mod-v ?s ?l _ _ ?eco ?hansen)
        (= ?pd first-idx)
        (= ecoid ?eco)
        (:distinct false))))

(defmain prebeta
  "do everything but actually estimate the beta vector - we want the data all in one file we can download"
  [tmp-root static-path final-path out-path ecoid]
  (let [est-map (forma-run-parameters "500-16")]
    (workflow [tmp-root]              
              genbetas
              ([]
                 (?- (hfs-seqfile out-path :sinkmode :replace)
                     (sink-eco est-map
                               (hfs-seqfile final-path)
                               (hfs-seqfile static-path)
                               ecoid))))))

(comment
  "Run this:"
  (prebeta "/user/hadoop/checkpoint"
               "s3n://formaresults/staticbuckettemp"
               "s3n://formaresults/finalbuckettemp"
               "s3n://formaresults/ecofeaturemat"
               40102))

(defn my-info-mat
  [feature-mat]
  (let [fm (log/to-double-matrix feature-mat)
        tp (.transpose fm)]
    (vec (.toArray (.mmul tp fm)))))

(defbufferop [local-info-matrix]
  [tuples]
  (let [val-mat      (map second tuples) 
        neighbor-mat (map last tuples)
        feature-mat  (map log/unpack-feature-vec val-mat neighbor-mat)]
    [[(first (last (repeatedly 1 #(my-info-mat feature-mat))))]]))

(defn run-local-info
  [in]
  (let [src (hfs-seqfile in)]
    (??<- [?mat]
          (src ?hansen ?val ?neighbor-val)
          (local-info-matrix ?hansen ?val ?neighbor-val :> ?mat))))

(defn simple-beta-generator
  "query to return the beta vector associated with an ecoregion"
  [{:keys [t-res est-start ridge-const convergence-thresh max-iterations]} src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?beta]
        (src ?hansen ?val ?neighbor-val)
        (log/logistic-beta-wrap [ridge-const convergence-thresh max-iterations]
                                ?hansen ?val ?neighbor-val :> ?beta)
        (:distinct false))))

(defmain run-simple-beta-generator
  [tmp-root in-path out-path]
  (let [est-map (forma-run-parameters "500-16")]
    (workflow [tmp-root]              
              genbetas
              ([]
                 (?- (hfs-seqfile out-path :sinkmode :replace)
                     (simple-beta-generator est-map
                                            (hfs-seqfile in-path)))))))

(comment
  (run-simple-beta-generator "/user/hadoop/checkpoint"
                             "s3n://formaresults/ecofeaturemat"
                             "s3n://formaresults/ecobetatests"))

;; (run-local-info "/Users/robin/Downloads/eco-40103-small")

;; (local-beta-generator (forma-run-parameters "500-16"))
;; (use 'forma.hadoop.jobs.scatter)

(defn my-info-mat
  [feature-mat]
  (let [fm (log/to-double-matrix feature-mat)
        tp (.transpose fm)]
    (vec (.toArray (.mmul tp fm)))))

(defbufferop [eco-bufferop [r c m]]
  [tuples]
  (let [val-mat      (map second tuples) 
        neighbor-mat (map last tuples)
        feature-mat  (map log/unpack-feature-vec val-mat neighbor-mat)]
    [[(first feature-mat)]]))

(defbufferop [info-matrix-bufferop [r c m]]
  [tuples]
  (let [val-mat      (map second tuples) 
        neighbor-mat (map last tuples)
        feature-mat  (map log/unpack-feature-vec val-mat neighbor-mat)]
    [[(first (last (repeatedly 1 #(my-info-mat feature-mat))))]]))

(defn buffer-generator
  "query to return the beta vector associated with an ecoregion"
  [{:keys [t-res est-start ridge-const convergence-thresh max-iterations]} src]
  (let [first-idx (date/datetime->period t-res est-start)]
    (<- [?beta]
        (src ?hansen ?val ?neighbor-val)
        ;;        (eco-bufferop [ridge-const convergence-thresh max-iterations]
        ;;              ?hansen ?val ?neighbor-val :> ?beta)
        (info-matrix-bufferop [ridge-const convergence-thresh max-iterations]
                      ?hansen ?val ?neighbor-val :> ?beta)
        
        (:distinct false))))

(defmain run-buffer-generator
  [tmp-root in-path out-path]
  (let [est-map (forma-run-parameters "500-16")]
    (workflow [tmp-root]              
              genbetas
              ([]
                 (?- (hfs-seqfile out-path :sinkmode :replace)
                     (buffer-generator est-map
                                       (hfs-seqfile in-path)))))))

(comment
  (let [src (hfs-seqfile "s3n://formaresults/ecofeaturemat")]
  (??<- [?count]
        (src ?hansen ?val ?neighbor)
        (c/count ?count))))