(ns forma.postprocess.paper
  (:use [cascalog.api]
        [forma.reproject :only (modis->latlon)]
        [incanter.core :only (view)]
        [incanter.charts :only (histogram)])
  (:require [cascalog.logic.ops :as c]))

(def forma-src
  [[0 0 50 50 2006 82]
   [0 0 50 50 2007 83]
   [0 0 50 50 2008 83]
   [0 0 50 50 2009 92]
   [0 0 50 50 2010 99]
   [0 0 50 51 2006 10]
   [0 0 50 51 2007 10]
   [0 0 50 51 2008 10]
   [0 0 50 51 2009 11]
   [0 0 50 51 2010 12]
   [0 0 50 55 2006 75]
   [0 0 50 55 2007 75]
   [0 0 50 55 2008 75]
   [0 0 50 55 2009 75]
   [0 0 50 55 2010 75]])

(def prodes-src
  "Note that there are MODIS pixels that have PRODES hits that are not
included in the FORMA data set, perhaps because VCF<25"
  [[0 0 50 50 2006 200]
   [0 0 50 50 2007 210]
   [0 0 50 50 2008 210]
   [0 0 50 50 2009 220]
   [0 0 50 50 2010 220]
   [0 0 50 51 2006 5]
   [0 0 50 51 2007 5]
   [0 0 50 51 2008 5]
   [0 0 50 51 2009 5]
   [0 0 50 51 2010 5]
   [0 0 50 52 2006 10]
   [0 0 50 52 2007 4]
   [0 0 50 52 2008 10]
   [0 0 50 52 2009 11]
   [0 0 50 52 2010 11]])

(defn prodes-of-hits
  [prodes-src forma-src year forma-thresh]
  (??<- [!!prodes]
        (forma-src  ?h ?v ?s ?l ?year ?forma)
        (prodes-src ?h ?v ?s ?l ?year !!prodes)
        (= year ?year)
        (< ?h 16)
        (> ?forma forma-thresh)))

(defn avg-prodes
  [prodes-src forma-src year forma-thresh]
  (let [prodes-seq (flatten (prodes-of-hits prodes-src forma-src year forma-thresh))
        n (count prodes-seq)
        sum (reduce + (filter #(not (nil? %)) prodes-seq))]
    (double (/ sum n))))

(defn describe-hits
  [year forma-thresh]
  (let [res (??<- [?prodes]
                  (forma-src  ?h ?v ?s ?l ?year ?forma)
                  (prodes-src ?h ?v ?s ?l ?year ?prodes)
                  (= year ?year)
                  (> ?forma forma-thresh))]
    (view (histogram (map first res)))))

(defn get-forma [src-path year forma-thresh]
  (let [src (hfs-seqfile src-path)]
    (<- [?lat ?lon ?forma]
        (src ?h ?v ?s ?l ?year ?forma)
        (modis->latlon "500" ?h ?v ?s ?l :> ?lat ?lon)
        (> ?forma forma-thresh)
        (= ?year year))))

(defn hist-forma
  [src-path year forma-thresh]
  (let [src (hfs-seqfile src-path)
        forma-seq (flatten
                   (??<- [?forma]
                         (src ?h ?v ?s ?l ?year ?forma)
                         (= ?year year)
                         (> ?forma forma-thresh)))]
    (view (histogram forma-seq :nbins 40))))

(defn hist-prodes
  [src-path year]
  (let [src (hfs-seqfile src-path)
        prodes-seq (flatten
                   (??<- [?prodes]
                         (src ?h ?v ?s ?l ?year ?prodes)
                         (= ?year year)))]
    (view (histogram prodes-seq :nbins 40))))




;; Dual histogram: for a given FORMA threshold, say 50%, a histogram
;; of the number of PRODES hits for MODIS pixels above /and/ below the
;; threshold.

;; For a given PRODES threshold (e.g., 25 count in a MODIS pixel), the
;; distribution of FORMA probabilities


(defn get-data []
  (let [src (hfs-seqfile "/home/dan/Downloads/prodis-modis")]
    (<- [?mod-h ?mod-v ?s ?l ?forma ?prodes]
        (src ?mod-h ?mod-v ?s ?l ?forma ?prodes))))

;; (def a (first (??- (get-data))))

;; (defn examine-prodes []
;;   (let [src (vec a)]
;;     (<- [?ct]
;;         (src _ _ _ _ ?forma ?prodes))))


;; (defn examine-prodes []
;;   (for [[_ _ _ _ forma prodes] a] [forma prodes]))


;; (defn get-hist
;;   [result-pairs]
;;   (for [[forma prodes] result-pairs :when (< forma 50)]
;;     prodes))
