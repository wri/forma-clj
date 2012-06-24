(ns forma.hadoop.jobs.local
  "Namespace for arbitrary queries."
  (:use cascalog.api
        forma.hoptree
        [forma.reproject :only (modis->latlon)]
        [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)])
  (:require [cascalog.ops :as c]
            [forma.matrix.utils :as util]
            [forma.matrix.walk :as walk]
            [forma.reproject :as r]
            [forma.thrift :as thrift]
            [forma.schema :as schema]
            [incanter.charts :as chart]
            [incanter.stats :as stat]
            [incanter.core :as i]
            [forma.trends.stretch :as stretch]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]
            [forma.date-time :as date]
            [forma.trends.analysis :as analyze]
            [forma.classify.logistic :as log])
  (:import [org.jblas FloatMatrix MatrixFunctions Solve DoubleMatrix]))

(def base-path "dev/testdata/select/")
(def dyn-path (str base-path "dynamic/"))
(def stat-path (str base-path "static/"))
(def fire-path (str base-path "fires/"))

(defn shorten-vec
  "returns a shortened vector by cutting off the tail of the supplied
  collection that is prepped to return a cascalog field"
  [len coll & {:keys [zero-index]
               :or {zero-index 0}}]
  [(vec (take len coll))
   (+ len zero-index)])

(defn gen-pixel-features
  "returns a lazy sequence of vectors, each with the pixel identifier,
  training label, and dynamic features used for estimation; all pixels
  with VCF < 25 are filtered out; used to replicate FORMA results.

  Example usage:
  (count (create-feature-vectors 134)) => number of pixels
  ;; where 135 is the length of the training period at 16-day res."
  [num-pds]
  (let [dyn-src (hfs-seqfile dyn-path)
        stat-src (hfs-seqfile stat-path)]
    (<- [?loc ?forma-val ?hansen ?pd]
        (dyn-src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?ndvi-ts ?precl-ts ?reli-ts)
        (stat-src ?s-res ?mod-h ?mod-v ?sample ?line ?gadm ?vcf ?ecoid ?hansen ?border)
        (shorten-vec num-pds ?ndvi-ts :zero-index 693 :> ?ndvi ?pd)
        (analyze/hansen-stat ?ndvi :> ?han-stat)
        (analyze/long-stats  ?ndvi :> ?long ?tstat)
        (analyze/short-stat 30 10 ?ndvi :> ?short)
        (thrift/FireValue* 0 0 0 0 :> ?fire)
        (thrift/ModisPixelLocation* "500" ?mod-h ?mod-v ?sample ?line :> ?loc)
        (thrift/FormaValue* ?fire ?short ?long ?tstat ?han-stat :> ?forma-val)
        (>= ?vcf 25))))

(defmapcatop neighbors
  [idx & {:keys [sres] :or {sres "500"}}]
  (neighbor-idx sres (GlobalIndex* idx)))

(defn neighbor-src [pixel-modis-src sres]
  (<- [?neigh-id]
      (pixel-modis-src ?gadm ?h ?v ?s ?l ?val)
      (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
      (global-index sres ?tile-pixel :> ?idx)
      (neighbors ?idx :> ?neigh-id)
      (:distinct true)))

(defn window-attribute-src [pixel-modis-src full-modis-src sres]
  (let [n-src (neighbor-src pixel-modis-src sres)]
    (<- [?gadm ?idx ?val]
        (n-src ?idx)
        (full-modis-src ?gadm ?h ?v ?s ?l ?val)
        (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
        (global-index sres ?tile-pixel :> ?idx)
        (:distinct true))))

(defn window-map
  "note height and width take into account the zero index."
  [sres global-idx-coll]
  (let [rowcol (fn [x] (global-rowcol sres (GlobalIndex* x)))
        [min-r min-c] (rowcol (reduce min global-idx-coll))
        [max-r max-c] (rowcol (reduce max global-idx-coll))]
    {:topleft-rowcol [min-r min-c]
     :height (inc (- max-r min-r))
     :width  (inc (- max-c min-c))}))

(defn global-coll->window-coll
  [sres wmap idx-val]
  (map (fn [[idx val]]
         [(window-idx sres wmap (GlobalIndex* idx)) val])
       idx-val))

(defn create-window
  [window-coll & {:keys [nrows ncols] :or {nrows 0 ncols 0}}]
  {:pre [(> nrows 0) (> ncols 0)]
   :post [(not-any? nil? (last %))]}
  (let [len (* nrows ncols)]
    (partition ncols
               (util/sparse-expander nil window-coll :length len))))

(defn process-neighbors
  [sres wmap f window-mat]
  (let [win-col (map-indexed vector
                             (walk/windowed-function 1 f window-mat))]
    (map (fn [[idx val]]
           [(global-index "500" (WindowIndex* idx) wmap) val])
         win-col)))

(defbufferop [assign-vals [sres]]
  [tuples]
  (let [wmap (window-map sres (map first tuples))
        window-coll (global-coll->window-coll sres wmap tuples)
        window-mat (create-window
                    (sort-by first window-coll)
                    :nrows (wmap :height)
                    :ncols (wmap :width))
        my-sum (fn [& args] (i/sum args))]
    (process-neighbors sres wmap my-sum window-mat)))



(defn mine [sres]
  (let [w-src (window-attribute-src sm-src lg-src sres)]
    (<- [?group ?new-idx ?new-val]
        (w-src ?group ?idx ?val)
        (assign-vals [sres] ?idx ?val :> ?new-idx ?new-val))))

(defn filtered-mine []
  (let [mine-src (mine "500")]
    (??<- [?group ?idx ?val]
          (mine-src ?group ?idx ?val)
          (sm-src _ ?h ?v ?s ?l _)
          (TileRowCol* ?h ?v ?s ?l :> ?tile-pixel)
          (global-index "500" ?tile-pixel :> ?idx))))

(def sm-src [["a" 0 0 20 20 1]
             ["a" 0 0 21 20 1]
             ["a" 0 0 20 21 1]
             ["a" 0 0 21 21 1]])

(def lg-src [["a" 0 0 19 19 1]
             ["a" 0 0 19 20 1]
             ["a" 0 0 19 21 1]
             ["a" 0 0 19 22 1]
             ["a" 0 0 20 19 1]
             ["a" 0 0 20 20 1]
             ["a" 0 0 20 21 1]
             ["a" 0 0 20 22 1]
             ["a" 0 0 21 19 1]
             ["a" 0 0 21 20 1]
             ["a" 0 0 21 21 1]
             ["a" 0 0 21 22 1]
             ["a" 0 0 22 19 1]
             ["a" 0 0 22 20 1]
             ["a" 0 0 22 21 1]
             ["a" 0 0 22 22 1]
             ["a" 0 0 23 19 1]
             ["a" 0 0 23 20 1]
             ["a" 0 0 23 21 1]
             ["a" 0 0 23 22 1]
             ["a" 0 0 24 19 1]
             ["a" 0 0 24 20 1]
             ["a" 0 0 24 21 1]
             ["a" 0 0 24 22 1]])


;; (defn grab-ndvi [num-pds]
;;   (let [dyn-src (hfs-seqfile dyn-path)
;;         stat-src (hfs-seqfile stat-path)]
;;     (??<- [?mod-h ?mod-v ?sample ?line ?hansen ?vcf ?ndvi]
;;           (dyn-src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?ndvi-ts ?precl-ts ?reli-ts)
;;           (stat-src ?s-res ?mod-h ?mod-v ?sample ?line ?gadm ?vcf ?ecoid ?hansen)
;;           (shorten-ndvi num-pds ?ndvi-ts :> ?ndvi)
;;           (= ?hansen 100))))


;; (defn label-sequence [casc-out]
;;   (log/to-double-rowmat (vec (map #(/ (nth % 4) 100) casc-out))))

;; (defn feat-sequence [casc-out]
;;   (log/to-double-matrix
;;    (vec (map (comp (partial into [1]) vec (partial drop 5)) casc-out))))

;; (defn probs []
;;   (let [casc-output (create-feature-vectors 135)
;;         label (label-sequence casc-output)
;;         feat  (feat-sequence casc-output)
;;         beta  (log/to-double-rowmat (log/logistic-beta-vector label feat 1e-8 1e-6 250))]
;;     (log/probability-calc beta feat)))

;; (defn xy-hansen []
;;   (let [casc-out (create-feature-vectors 135)
;;         pixel-loc (map (partial take 4) casc-out)
;;         latlon    (map (partial apply modis->latlon "500") pixel-loc)
;;         labels    (map #(/ (nth % 4) 100) casc-out)
;;         data      (map cons labels latlon)
;;         hits (for [x data :when (= 1 (first x))] (rest x))]
;;     (apply map vector hits)))

;; (defn xy-hits []
;;   (let [casc-out (create-feature-vectors 135)
;;         pixel-loc (map (partial take 4) casc-out)
;;         latlon    (map (partial apply modis->latlon "500") pixel-loc)
;;         label (label-sequence casc-out)
;;         feat  (feat-sequence casc-out)
;;         beta  (log/to-double-rowmat (log/logistic-beta-vector label feat 1e-8 1e-6 250))
;;         p (vec (.toArray (log/probability-calc beta feat)))
;;         data  (map cons p latlon)
;;         hits     (for [x data :when (> (first x) 0.5)] (rest x))]
;;     (apply map vector hits)))

;; (defn xy-new-hits [num]
;;   (let [casc-out (create-feature-vectors 135)
;;         pixel-loc (map (partial take 4) casc-out)
;;         latlon    (map (partial apply modis->latlon "500") pixel-loc)
;;         label (label-sequence casc-out)
;;         feat  (feat-sequence casc-out)
;;         new-feat (feat-sequence (create-feature-vectors num))
;;         beta  (log/to-double-rowmat (log/logistic-beta-vector label feat 1e-8 1e-6 250))
;;         p (vec (.toArray (log/probability-calc beta new-feat)))
;;         data  (map cons p latlon)
;;         hits     (for [x data :when (> (first x) 0.5)] (rest x))]
;;     (apply map vector hits)))

;; (defn plot-forma []
;;   (let [xy-forma (xy-hits)
;;         x (first xy-forma)
;;         y (second xy-forma)]
;;     (doto (chart/scatter-plot x y)
;;       (chart/set-stroke-color java.awt.Color/blue)
;;       (chart/set-y-range 101.725 101.975)
;;       (chart/set-x-range 0.7 0.95))))

;; (defn plot-hansen []
;;   (let [xy-han (xy-hansen)
;;         x (first xy-han)
;;         y (second xy-han)]
;;     (prn (count y))
;;     (doto (chart/scatter-plot x y)
;;       (chart/set-stroke-color java.awt.Color/red)
;;       (chart/set-y-range 101.725 101.975)
;;       (chart/set-x-range 0.7 0.95))))

;; (defn plot-new-forma [num]
;;   (let [xy-forma (xy-new-hits num)
;;         x (first xy-forma)
;;         y (second xy-forma)]
;;     (prn (count y))
;;     (doto (chart/scatter-plot x y)
;;       (chart/set-stroke-color java.awt.Color/cyan)
;;       (chart/set-y-range 101.725 101.975)
;;       (chart/set-x-range 0.7 0.95))))

;; (defn prob-seq [num]
;;   (let [casc-out (create-feature-vectors 135)
;;         pixel-loc (map (partial take 4) casc-out)
;;         latlon    (map (partial apply modis->latlon "500") pixel-loc)
;;         label (label-sequence casc-out)
;;         feat  (feat-sequence casc-out)
;;         new-feat (feat-sequence (create-feature-vectors num))]
;;     new-feat))
