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
      (thrift/unpack* ?data :> ?vals)
      (p/index ?vals :> ?pixel-idx ?val)
      (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?id ?size)
      (r/tile-position ?s-res ?size ?id ?pixel-idx :> ?sample ?line)

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
  [coll]
  [(vec (map round coll))])

(defn adjusted-precl-tap
  "Document... returns a tap that adjusts for the incoming
  resolution."
  [ts-pail-path s-res base-t-res t-res]
  (let [precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res)]
    (if (= t-res base-t-res)
      precl-tap
      (<- [?path ?adjusted-pixel-chunk]
          (precl-tap ?path ?pixel-chunk)
          (thrift/unpack ?pixel-chunk :> ?name ?in-pix-loc ?ts ?t-res !date)
          (thrift/unpack ?in-pix-loc :> ?s-res ?mod-h ?mod-v ?sample ?line)
          (stretch/ts-expander base-t-res t-res ?ts :> ?expanded-ts)
          (map-round ?expanded-ts :> ?rounded-ts)
          (thrift/ModisPixelLocation* ?s-res ?mod-h ?mod-v ?sample ?line :> ?out-pix-loc)
          (thrift/DataChunk* ?name ?out-pix-loc ?rounded-ts ?t-res !date :> ?adjusted-pixel-chunk)
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
  [ts-pail-path s-res base-t-res t-res]
  (let [precl-tap (constrained-tap ts-pail-path "precl" s-res base-t-res)]
    (<- [?path ?adjusted-pixel-chunk]
        (precl-tap ?path ?pixel-chunk)
        (thrift/unpack ?pixel-chunk :> ?name ?pixel-loc ?ts ?t-res ?date)
        (thrift/unpack ?pixel-loc :> ?s-res ?h ?v ?sample ?line)
        (expand-rain-pixel ?sample ?line :> ?s ?l)
        (stretch/ts-expander base-t-res t-res ?ts :> ?expanded-ts)
        (thrift/ModisPixelLocation* "500" ?h ?v ?s ?l :> ?new-pixel-loc)
        (thrift/DataChunk* ?name ?new-pixel-loc ?expanded-ts t-res :> ?adjusted-pixel-chunk)
        (:distinct false))))

(defn first-half-query
  "Poorly named! Returns a query that generates a number of position
  and dataset identifier"
  [pail-path ts-pail-path out-path run-key]
  (let [{:keys [s-res t-res est-end] :as forma-map} (forma-run-parameters run-key)]
    (assert forma-map (str run-key " is not a valid run key!"))
    (with-job-conf {"mapreduce.jobtracker.split.metainfo.maxsize" 100000000000}
      (?- (hfs-seqfile out-path :sinkmode :replace)
          (forma/forma-query
           forma-map
           (constrained-tap ts-pail-path "ndvi" s-res t-res)
           (constrained-tap ts-pail-path "reli" s-res t-res)
           (new-adjusted-precl-tap ts-pail-path "1000" "32" t-res)
           (constrained-tap pail-path "vcf" s-res "00")
           (tseries/fire-query pail-path
                               t-res
                               "2000-11-01"
                               est-end))))))

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
                     (<- [?subpail ?chunk]
                         ((constrained-tap pail-path "vcf" s-res "00") ?subpail ?chunk))))

              ndvi-step
              ([:tmp-dirs ndvi-path]
                 (?- (hfs-seqfile ndvi-path)
                     (mk-filter vcf-path
                                (constrained-tap
                                 ts-pail-path "ndvi" s-res t-res))))

              fire-step ([:tmp-dirs fire-path]
                           (?- (hfs-seqfile fire-path)
                               (tseries/fire-query pail-path
                                                   t-res
                                                   "2000-11-01"
                                                   est-end)))

              adjustfires
              ([:tmp-dirs adjusted-fire-path]
                 (?- (hfs-seqfile adjusted-fire-path)
                     (forma/fire-tap est-map (hfs-seqfile fire-path))))

              rain-step
              ([:tmp-dirs rain-path]
                 (?- (hfs-seqfile rain-path)
                     (mk-filter vcf-path
                                (new-adjusted-precl-tap
                                 ts-pail-path s-res "32" t-res))))

              reli-step
              ([:tmp-dirs reli-path]
                 (?- (hfs-seqfile reli-path)
                     (mk-filter vcf-path
                                (constrained-tap
                                 ts-pail-path "reli" s-res t-res))))
              
              adjustseries
              ([:tmp-dirs adjusted-series-path]
                 "Adjusts lengths of all timeseries so they all cover the same
                  time spans."
                 (with-job-conf {"mapred.min.split.size" 805306368}
                   (?- (hfs-seqfile adjusted-series-path)
                       (forma/dynamic-filter (hfs-seqfile ndvi-path)
                                             (hfs-seqfile reli-path)
                                             (hfs-seqfile rain-path)))))

              cleanseries ([:tmp-dirs clean-series]
                             "Runs the trends processing."
                             (?- (hfs-seqfile clean-series)
                                 (forma/dynamic-clean
                                  est-map (hfs-seqfile adjusted-series-path))))

              trends ([:tmp-dirs trends-path]
                        "Runs the trends processing."
                        (?- (hfs-seqfile trends-path)
                            (forma/analyze-trends
                             est-map (hfs-seqfile clean-series))))

              mid-forma ([:tmp-dirs forma-mid-path
                          :deps [trends adjustfires]]
                           (?- (hfs-seqfile forma-mid-path)
                               (forma/forma-tap (hfs-seqfile trends-path)
                                                (hfs-seqfile adjusted-fire-path))))
              
              final-forma
              ([] (let [names ["?s-res" "?period" "?mod-h" "?mod-v"
                               "?sample" "?line" "?forma-val"]
                        mid-src (-> (hfs-seqfile forma-mid-path)
                                    (name-vars names))]
                    (?- (hfs-seqfile out-path)
                        (forma/forma-query est-map mid-src)))))))

(defmain ultrarunner
  [tmp-root eco-beta-path full-beta-path static-path dynamic-path out-path country-or-eco pre-beta-out-path]
  (let [est-map (forma-run-parameters "500-16")]
    (workflow [tmp-root]              
              ;; genbetas
              ;; ([]
              ;;    (let [beta-path (if (= "eco" country-or-eco)
              ;;                      eco-beta-path
              ;;                      full-beta-path)]
              ;;      (?- (hfs-seqfile beta-path)
              ;;          (forma/beta-generator est-map
              ;;                                (hfs-seqfile final-path)
              ;;                                (hfs-seqfile static-path)))))
              applybetas
              ([] (?- (hfs-seqfile out-path :sinkmode :replace)
                      (forma/forma-estimate (hfs-seqfile eco-beta-path)
                                            (hfs-seqfile dynamic-path)
                                            (hfs-seqfile
                                            static-path)))))))

(defn run-forma-estimate
  [beta-src dynamic-src static-src out-loc trap-path period]
  (?- (hfs-seqfile out-loc :sinkmode :replace)
      (forma/forma-estimate 
       (hfs-seqfile beta-src)
       (hfs-seqfile dynamic-src)
       (hfs-seqfile static-src)
       (hfs-seqfile trap-path)
       period)))

(comment
  (run-forma-estimate "s3n://formaresults/ecobetatemp"
                      "s3n://formaresults/finalbuckettemp"
                      "s3n://formaresults/staticbuckettemp"
                      "s3n://formaresults/finaloutput"
                      "s3n://formaresults/trapped"
                      827))

(defn within-tile?
  [[target-h target-v] h v]
  (let [h-target 28
        v-target 8]
    (and (= h h-target) (= v v-target))))

(def bbox {:min-lat 0.71
            :max-lat 0.91
            :min-lon 101.75
           :max-lon 101.95})

(defn within-bbox?
  [{:keys [min-lat max-lat min-lon max-lon]} s-res h v s l]
  (let [[lat lon] (r/modis->latlon s-res h v s l)]
    (and (< lat max-lat)
         (> lat min-lat)
         (< lon max-lon)
         (> lon min-lon))))

(comment
  (let [src (hfs-seqfile "s3n://formaexperiments/preadjusted/reli")
      bbox {:min-lat 0.71
            :max-lat 0.91
            :min-lon 101.75
            :max-lon 101.95}]
  (??<- [?count]
        (src ?s-res ?mod-h ?mod-v ?sample ?line ?start _)
;;        (= 693 ?start)
        (within-bbox? bbox ?s-res ?mod-h ?mod-v ?sample ?line)
        (c/count ?count))))
         
(defn get-date
  [data-chunk]
  (.getDate data-chunk))

(defn unpack-loc
  [dc]
  (let [cl (->> dc .getLocationProperty .property .getChunkLocation)]
    [(.getTileH cl)
     (.getTileV cl)
     (.getChunkID cl)]))

(defn blossom-static
  [src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?pixel-val]
      (src ?dc)
      (thrift/unpack ?dc :> _ ?loc ?chunk-data _ !date)
      (thrift/unpack* ?chunk-data :> ?vals)
      (p/index ?vals :> ?pixel-idx ?pixel-val)
      (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?id ?size)
      (r/tile-position ?s-res ?size ?id ?pixel-idx :> ?sample ?line)))

(defn get-static-tile-pixels
 [dataset pail-path s-t-res]
 (let [tile-vec [28 8]
        src (split-chunk-tap pail-path [dataset s-t-res])]
       (<- [?dc]
           (src _ ?dc)
           (unpack-loc ?dc :> ?mod-h ?mod-v _)
           (within-tile? tile-vec ?mod-h ?mod-v))))

(defn get-ts-tile-pixels
 [dataset pail-path s-t-res]
 (let [tile-vec [28 8]
        src (split-chunk-tap pail-path [dataset s-t-res])]
       (<- [?dc]
           (src _ ?dc)
           (unpack-loc ?dc :> ?mod-h ?mod-v _)
           (within-tile? tile-vec ?mod-h ?mod-v)
           (get-date ?dc :> ?date-str)
           (= "2012-01-01" ?date-str))))

(comment
  (let [dataset "ndvi"]
    (?- (hfs-seqfile (str "s3n://formareset/2808/" dataset) :sinkmode :replace)
        (get-ts-tile-pixels dataset "s3n://pailbucket/masterpail" "500-16"))))

(comment
         bbox {:min-lat 0.71
             :max-lat 0.91
             :min-lon 101.75
             :max-lon 101.95})
;;           (blossom-static ?dc :> ?s-res ?mod-h ?mod-v ?sample ?line ?pixel-val)
;;           (within-bbox? bbox ?s-res ?mod-h ?mod-v ?sample ?line)


(defn screen-rain-by-date
  [dataset pail-path]
  (let [src (split-chunk-tap pail-path ["precl" "500-32"])]
       (<- [?dc]
           (src _ ?dc)
           (get-date ?dc :> ?date-str)
           (= "2012-01-01" ?date-str))))

(defn count-vcf-chunks
 [pail-path]
  (let [src (split-chunk-tap pail-path ["vcf" "500-00"])]
       (<- [?mod-h ?mod-v ?count]
           (src _ ?dc)
           (unpack-loc ?dc :> ?mod-h ?mod-v ?id)
           (= 28 ?mod-h)
           (= 8 ?mod-v)
           (c/count ?count))))

(defn count-precl-chunks
  [pail-path]
  (let [src (split-chunk-tap pail-path ["precl" "500-32"])]
       (<- [?mod-h ?mod-v ?count]
           (src _ ?dc)
           (unpack-loc ?dc :> ?mod-h ?mod-v ?id)
           (get-date ?dc :> ?date-str)
           (= 28 ?mod-h)
           (= 8 ?mod-v)
           (= "2012-01-01" ?date-str)
           (c/count ?count))))

(defmain CountVcf [pail-path]
  (??- (count-vcf-chunks pail-path)))


(defn blossom
  [vcf-src vcf-limit chunk-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series]
      (chunk-src _ ?ts-chunk)
      (vcf-src _ ?vcf-chunk)      

      ;; mirrors p/blossom-chunk, but doesn't have "invalid predicate" error
      ;; (p/blossom-chunk ?vcf-chunk :> ?s-res ?mod-h ?mod-v ?sample ?line ?vcf)
      
      ;; unpack ts object
      (thrift/unpack ?ts-chunk :> _ ?ts-loc ?ts-data _ !date)
      (thrift/unpack ?ts-data :> ?start-idx _ ?ts-array)
      (thrift/unpack* ?ts-array :> ?series)
      (thrift/unpack ?ts-loc :> ?s-res ?mod-h ?mod-v ?sample ?line)

      ;; filter on vcf-limit
      (>= ?vcf vcf-limit)
      (:distinct false)))
