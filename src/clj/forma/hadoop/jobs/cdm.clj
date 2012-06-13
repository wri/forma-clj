(ns forma.hadoop.jobs.cdm
  "Functions and Cascalog queries for converting data into map tile coordinates."
  (:use [cascalog.api]
        [forma.gfw.cdm :only (latlon->tile, read-latlon, latlon-valid?)]
        [forma.utils :only (positions)])
  (:require [forma.postprocess.output :as o]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [cascalog.ops :as c]))

;; TODO: https://github.com/reddmetrics/forma-clj/pull/16#r782582
(defbufferop min-period
  "Returns the minimum value in tuples."
  [tuples]
  [(reduce min (map first tuples))])

(defn first-hit
  "Returns the index of the first value in a vector of numbers that is greater
  than or equal to a threshold.

  Arguments:
    thresh - The threshold value.
    series - A vector of numbers.

  Example usage:
    > (first-hit 5 [1 2 3 4 5 6 7 8 9 10])
    > 4
  "
  [thresh series]
  (first (positions (partial <= thresh) series)))

(defn hansen->cdm
  "Returns a Cascalog query that transforms Hansen data into map tile coordinates.

  Arguments:
    src - The source tap.
    zoom - The map zoom level.
    tres - The temporal resolution."
  [src zoom tres]
  (let [epoch (date/datetime->period tres "2000-01-01")
        hansen (date/datetime->period tres "2010-12-31")
        period (- hansen epoch)]
    (<- [?x ?y ?z ?p]
        (src ?longitude ?latitude _)
        (read-latlon ?latitude ?longitude :> ?lat ?lon)
        (latlon-valid? ?lat ?lon) ;; Skip if lat/lon invalid.
        (identity period :> ?p)
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))

(defn forma->cdm
  "Returns a Cascalog query that transforms FORMA data into map tile coordinates.

  Arguments:
    start - Start period date string.
    src - The source tap for FORMA data.
    thresh - The threshold number for valid detections.
    tres - The temporal resolution as a string.
    tres-out - The output temporal resolution as a string.
    zoom - The map zoom level."
  [src zoom tres tres-out start thresh]
  (let [epoch (date/datetime->period tres-out "2000-01-01")
        start-period (date/datetime->period tres start)]
    (<- [?x ?y ?z ?p]
        (src ?sres ?modh ?modv ?s ?l ?prob-series)
        (o/clean-probs ?prob-series :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ start-period ?first-hit-idx :> ?period)
        (date/convert-period-res tres tres-out ?period :> ?period-new-res)
        (- ?period-new-res epoch :> ?rp)
        (min-period ?rp :> ?p)
        (r/modis->latlon ?sres ?modh ?modv ?s ?l :> ?lat ?lon)
        (latlon-valid? ?lat ?lon) ;; Skip if lat/lon invalid.
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))

(defn is-a-hit?
  [thresh series]
  (if (first-hit thresh series)
    true
    false))

(defn simple-clean
  [series]
  [(map (comp int (partial * 100)) series)])

(comment
  (defn forma->latlon-cdm
  "Returns a Cascalog query that transforms FORMA data into map tile coordinates.

  Arguments:
    start - Start period date string.
    src - The source tap for FORMA data.
    thresh - The threshold number for valid detections.
    tres - The temporal resolution as a string.
    tres-out - The output temporal resolution as a string.
    zoom - The map zoom level."
  [src zoom tres tres-out start thresh]
  (let [epoch (date/datetime->period tres-out "2000-01-01")
        start-period (date/datetime->period tres start)]
    (<- [?lat ?lon ?x ?y ?z ?p]
        (src ?sres ?modh ?modv ?s ?l ?prob-series)
        (o/clean-probs ?prob-series :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ start-period ?first-hit-idx :> ?period)
        (date/convert-period-res tres tres-out ?period :> ?period-new-res)
        (- ?period-new-res epoch :> ?rp)
        (min-period ?rp :> ?p)
        (r/modis->latlon ?sres ?modh ?modv ?s ?l :> ?lat ?lon)
        (latlon-valid? ?lat ?lon) ;; Skip if lat/lon invalid.
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))

(?- (hfs-textline "s3n://formaexperiments/boomshakalaka" :sinkmode :replace)
    (let [src (hfs-seqfile "s3n://formaexperiments/results")
      static-src (hfs-seqfile "s3n://formaexperiments/static")
      thresh 50]
  (<- [?lat ?lon ?first-hit-idx]
      (src ?sres ?modh ?modv ?s ?l ?prob-series)
;;      (static-src ?s-res ?mod-h ?mod-v ?s ?l ?gadm ?vcf ?eco ?hansen ?border)
      (o/clean-probs ?prob-series :> ?clean-series)
      (r/modis->latlon ?sres ?modh ?modv ?s ?l :> ?lat ?lon)
      (first-hit thresh ?clean-series :> ?first-hit-idx))))

(?- (hfs-seqfile "s3n://formaexperiments/ever-above-thresh" :sinkmode :replace)
    (let [src (hfs-seqfile "s3n://formaexperiments/results")
          thresh 0.5]
      (<- [?lat ?lon ?modh ?modv ?s ?l ?clean-series]
          (src ?sres ?modh ?modv ?s ?l ?prob-series)
          ;;      (static-src ?s-res ?mod-h ?mod-v ?s ?l ?gadm ?vcf ?eco ?hansen ?border)
          (is-a-hit? thresh ?prob-series)
          (simple-clean ?prob-series :> ?clean-series)
          ;;(o/clean-probs ?prob-series :> ?clean-series)
          (r/modis->latlon ?sres ?modh ?modv ?s ?l :> ?lat ?lon))))

;; (?- (hfs-seqfile "s3n://formaexperiments/results-cdm" :sinkmode :replace)
;;    (forma->latlon-cdm (hfs-seqfile "s3n://formaexperiments/results")
;;                       17 "16" "32" "2005-12-31" 50))

;; (?- (hfs-textline "/Users/robin/delete/cdm" :sinkmode :replace)
;;    (let [src (hfs-textline "/Users/robin/delete/forma17/forma17.txt")]
;;      (<- [?x ?y ?z ?min-p]
;;          (src ?line)
;;          (split ?line #"\t" :> ?x ?y ?z ?p)
;;          (min-period ?p :> ?min-p))))


)
