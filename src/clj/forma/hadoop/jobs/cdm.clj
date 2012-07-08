(ns forma.hadoop.jobs.cdm
  "Functions and Cascalog queries for converting data into map tile coordinates."
  (:use [cascalog.api]
        [cartodb.playground :only (insert-rows delete-all)]
        [forma.source.admin :only (gadm->iso)]
        [forma.gfw.cdm :only (latlon->tile, read-latlon, latlon-valid?)]
        [forma.utils :only (positions)])
  (:require [forma.postprocess.output :as o]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [cascalog.ops :as c]))

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
    (first-hit 5 [1 2 3 4 5 6 7 8 9 10]) => 4"
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
    gadm-src - a sequence file source with mod-h, mod-v, sample, line, and gadm
    thresh - The threshold number for valid detections.
    tres - The temporal resolution as a string.
    tres-out - The output temporal resolution as a string.
    zoom - The map zoom level."
  [src gadm-src zoom tres tres-out start thresh]
  (let [epoch (date/datetime->period tres-out "2000-01-01")
        start-period (date/datetime->period tres start)]
    (<- [?x ?y ?z ?p ?iso ?lat ?lon]
        (src ?sres ?modh ?modv ?s ?l ?prob-series)
        (gadm-src _ ?modh ?modv ?s ?l ?gadm)
        (gadm->iso ?gadm :> ?iso)
        (o/clean-probs ?prob-series :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ start-period ?first-hit-idx :> ?period)
        (date/convert-period-res tres tres-out ?period :> ?period-new-res)
        (- ?period-new-res epoch :> ?rp)
        (min-period ?rp :> ?p)
        (r/modis->latlon ?sres ?modh ?modv ?s ?l :> ?lat ?lon)
        (latlon-valid? ?lat ?lon)
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))
