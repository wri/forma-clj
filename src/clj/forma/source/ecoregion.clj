(ns forma.source.ecoregion
  (:use cascalog.api)
  (:require [clojure.java.io :as io]
            [forma.utils :as u]
            [cascalog.ops :as c]))

;; This namespace helps us deal with the fact that some ecoregions
;; have very sparse training data relative to the number of
;; pixels. In those ecoregions, the relative weights of the trends,
;; fires and other characteristics may be incorrect as predictors of
;; deforestation, or at worse, may not be calculable at all.

;; This namespace uses a dataset of David Wheeler's super-regions -
;; defined in WRI FORMA Note #15 (June 2013) - to replace a pixel's
;; ecoid with the id of a super-ecoregion that may be adequately 

;; Assumes use of 500m data. The number of pixels vs. number of hits
;; will be different for a different resolution.

(def HITS-THRESHOLD
  "Minimum value for share of Hansen hits in total pixels. Ecoregions
   with less than this will take the id of a super-ecoregion."
   0.005) ;; half a percent

(defn split-line
  [line]
  (seq (.split line "\t")))

(def ecoregion-coll
  "Collection of ecoregions and super-ecoregions, w/metadata, generated from
   tab-separated file. `rest` drops the header line."
  (-> "ecoregion_concordance.txt" io/resource io/reader line-seq rest))

(defn line->eco-dict
  "Parse a line of the ecoregion data and build a dictionary of regions and hit shares for each ecoid.

   Usage:
     (let [line 10101\t\"Admiralty Islands lowland rain forests\"\t\"Papua New Guinea\"\t\"Papua New Guinea\"\t21\t\"New Guinea and Islands\"\t4693\t6]
       (parse-line line))
     ;=> {10101 {:regionid 21, :hit-share 0.001278499893458342}}"
  [line]
  (let [[ecoid ecoregion country1 country2 regionid region pixels hits] (split-line line)
        [ecoid regionid pixels hits] (map #(Integer/parseInt %) [ecoid regionid pixels hits])
        hit-share (double (/ hits pixels))
        region-clean (clojure.string/replace region "\"" "")]
    {ecoid {:regionid regionid :pixels pixels :hits hits :hit-share hit-share :region region-clean}}))

(def eco-dict
  "Parse all lines in `ecoregion-coll` to create a dictionary of
   ecoids to regions and hit shares."
  (let [small-dicts (map line->eco-dict ecoregion-coll)]
    (apply merge small-dicts)))

(defn pixels-by-super-region
  [src]
  (<- [?region-id ?region ?pixels-sum]
      (src ?line)
      (split-line ?line :> _ _ _ _ ?regionid-str ?region ?pixels _)
      (read-string ?pixels :> ?pixels-int)
      (read-string ?regionid-str :> ?region-id)
      (c/sum ?pixels-int :> ?pixels-sum)))

(defn hits-by-super-region
  [src]
  (<- [?region-id ?region ?hits-int]
      (src ?line)
      (split-line ?line :> _ _ _ _ ?regionid-str ?region _ ?hits)
      (read-string ?hits :> ?hits-int)
      (read-string ?regionid-str :> ?region-id)
      (c/sum ?hits-int :> ?hits-sum)))

(defn summarize-super-regions
  [src]
  (let [pixel-count-src (pixels-by-super-region src)
        hit-count-src (hits-by-super-region src)]
    (<- [?regionid ?region-clean ?pixels-sum ?hits-sum ?share]
        (pixel-count-src ?regionid ?region ?pixels-sum)
        (hit-count-src ?regionid ?region ?hits-sum)
        (div ?hits-sum ?pixels-sum :> ?share)
        (clojure.string/replace ?region "\"" "" :> ?region-clean))))

;; causes out of memory error for some reason
(comment
  (def super-eco-dict
    (let [data (??- (summarize-super-regions ecoregion-coll))]
      (apply assoc {} (reduce concat (first data))))))

(defn get-super-ecoregion
  [ecoid]
  (:regionid (eco-dict ecoid)))

(defn get-hit-share
  [ecoid]
  (:hit-share (eco-dict ecoid)))

(defn get-ecoregion
  "Given an ecoid and `super-ecoregions` `true`, check whether that
  ecoregion meets the `HITS-THRESHOLD`. If so, return the original
  ecoid. Otherwise, return the super-region id."
  [ecoid & {:keys [super-ecoregions] :or {super-ecoregions false}}]
  (let [hit-share (get-hit-share ecoid)]
    (if (and super-ecoregions
             hit-share
             (>= HITS-THRESHOLD hit-share))
      (get-super-ecoregion ecoid)
      ecoid)))
