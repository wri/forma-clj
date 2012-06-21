(ns forma.hadoop.jobs.local
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.reproject :only (modis->latlon)]
        [forma.hadoop.pail :only (to-pail)]
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)])
  (:require [cascalog.ops :as c]
            [forma.matrix.utils :as util]
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

(defn global-dims [s-res]
  (let [num-pixels (r/pixels-at-res s-res)]
    (map (partial * num-pixels) [r/v-tiles r/h-tiles])))

(defn global-indexer
  "accepts a modis pixel coordinates at the tile level and returns a unique,
  global index; origin is at top left, as seen [here](goo.gl/pCgqF)"
  [h v s l & {:keys [s-res] :or {s-res "500"}}]
  (let [num-pixels (r/pixels-at-res s-res)
        col (+ (* h num-pixels) s)
        row (+ (* v num-pixels) l)
        [nrows ncols] (global-dims s-res)]
    (util/rowcol->idx nrows ncols row col)))

(defn valid-neighbor?
  [s-res idx]
  (let [max-dim (dec (r/pixels-at-res s-res))
        max-idx (global-indexer r/h-tiles r/v-tiles max-dim max-dim)]
    (if (and (pos? (inc idx))
             (< idx max-idx))
      idx
      nil)))

(defn neighbor-idx
  "returns the global indices of the 8 adjacent MODIS pixels"
  [pixel-idx & {:keys [s-res] :or {s-res "500"}}]
  (let [num-pixels (r/pixels-at-res s-res)
        up-one     (- pixel-idx (* r/h-tiles num-pixels))
        down-one   (+ pixel-idx (* r/h-tiles num-pixels))
        valid? (partial valid-neighbor? s-res)]
    (map valid? [pixel-idx (dec pixel-idx) (inc pixel-idx)
                 up-one    (dec up-one) (inc up-one)
                 down-one  (dec down-one) (inc down-one)])))

(defmapcatop neighbors
  [idx & {:keys [s-res] :or {s-res "500"}}]
  (neighbor-idx idx :s-res s-res))

;; (def test-modis-tap [[28 8 20 20]
;;                      [28 8 21 20]
;;                      [28 8 22 21]
;;                      [28 8 23 21]])
 
(def test-modis-tap [[0 0 20 20 1]
                     [0 0 21 20 1]
                     [0 0 20 21 1]
                     [0 0 21 21 1]])

(def test-modis-tap-all [[0 0 19 19 1]
                         [0 0 19 20 1]
                         [0 0 19 21 1]
                         [0 0 19 22 1]
                         [0 0 20 19 1]
                         [0 0 20 20 1]
                         [0 0 20 21 1]
                         [0 0 20 22 1]
                         [0 0 21 19 1]
                         [0 0 21 20 1]
                         [0 0 21 21 1]
                         [0 0 21 22 1]
                         [0 0 22 19 1]
                         [0 0 22 20 1]
                         [0 0 22 21 1]
                         [0 0 22 22 1]
                         [0 0 23 19 1]
                         [0 0 23 20 1]
                         [0 0 23 21 1]
                         [0 0 23 22 1]
                         [0 0 24 19 1]
                         [0 0 24 20 1]
                         [0 0 24 21 1]
                         [0 0 24 22 1]])

(defn neighbor-src [s-res]
  (<- [?neigh-id]
      (test-modis-tap ?h ?v ?s ?l ?val)
      (global-indexer ?h ?v ?s ?l :s-res s-res :> ?idx)
      (neighbors ?idx :> ?neigh-id)
      (:distinct true)))

(defbufferop bounding-indices
  [tuples]
  (let [coll (flatten tuples)]
    [[(reduce min coll) (reduce max coll)]]))

(defn buffer-test []
  (let [src (neighbor-src "500")]
    (??<- [?first ?last]
          (src ?idx)
          (bounding-indices ?idx :> ?first ?last))))

(defn idx->global-rowcol
  [idx & {:keys [s-res] :or {s-res "500"}}]
  (let [[nrows ncols] (global-dims s-res)]
    (util/idx->rowcol nrows ncols idx)))

(defbufferop box-dims
  [tuples]
  (let [coll (flatten tuples)
        min-idx (reduce min coll)
        max-idx (reduce max coll)]
    [(map (comp int inc i/abs -)
          (idx->global-rowcol min-idx)
          (idx->global-rowcol max-idx))]))

(defn box-test [neigh-src]
  (??<- [?y ?x]
        (neigh-src ?idx)
        (box-dims ?idx :> ?y ?x)))

(defn coll-rowcol->idx [nrow ncol coll]
  (let [partial-fn (fn [rc] (util/idx->rowcol nrow ncol (first rc) (second rc)))]
    (map partial-fn coll)))

(defbufferop [build-window [wr wc]]
  [tuples]
  (let [rowcol (map vec tuples)
        first-rowcol (first (sort-by second (sort-by first rowcol)))
        window-rowcol (map (partial map (comp int i/abs -) first-rowcol) rowcol)]
    ))

(defn top-left
  "accepts a sequence of tuples (row col) and returns the topmost,
  leftmost tuple"
  [coll]
  (first (sort-by second (sort-by first coll))))

(defn relative-transform
  "accepts a top-left tuple and then a collection of tuples; returns a
  transformed sequence where the tuples are constructed, relative to
  the top-left pixel"
  [top-left tuple]
  (let [abs-diff (comp int i/abs -)]
    (map abs-diff top-left tuple)))

(defbufferop [build-window [wr wc]]
  [tuples]
  (let [rowcol (map vec tuples)
        make-relative (partial relative-transform (top-left rowcol))
        rel-rowcol (map (comp vec make-relative) rowcol)]
   [[rel-rowcol]]))

(defn idx-src []
  (<- [?idx ?val]
      (test-modis-tap-all ?h ?v ?s ?l ?val)
      (global-indexer ?h ?v ?s ?l :> ?idx)))

(defn get-attributes [s-res]
  (let [n-src (neighbor-src s-res)
        idx-src (idx-src)
        [window-rows window-cols] (first (box-test n-src))]
    (??<- [?neigh-id ?val]
          (n-src ?neigh-id)
          (idx-src ?neigh-id ?val)
          (:distinct true))))



         ;; (idx->global-rowcol ?neigh-id :s-res s-res :> ?n-row ?n-col)
;; (build-window [window-rows window-cols] ?n-row ?n-col :> ?stand-in)


;; (defn gen-neighbor-features
;;   [num-pds]
;;   (let [forma-src (gen-pixel-features num-pds)]
;;     (<- [?mod-h ?mod-v ?sample ?line ?short]
;;           (forma-src ?loc ?forma-val ?hansen ?pd)
;;           (thrift/unpack ?forma-val :> _ ?short _ _ _)
;;           (thrift/unpack ?loc :> ?s-res ?mod-h ?mod-v ?sample ?line))))



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



