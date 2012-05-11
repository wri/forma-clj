(ns forma.hadoop.jobs.cdm
  "This namespace defines a Cascalog query to convert FORMA data in MODIS
  coordinates into map tile coordinates for use in Global Forest Watch
  visualizations."
  (:use [cascalog.api]
        [forma.gfw.cdm :only (latlon->tile)]
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
    > (first-hit 5 [1 2 3 4 5 6 7 8 9 10])
    > 4
  "
  [thresh series]
  (first (positions (partial <= thresh) series)))

(defn str->double [x]
  "Return x converted to a double value."
  (Double/parseDouble x))

(defn hansen->cdm
  "Returns a Cascalog query for converting Hansen data to the common data format."
  [hansen-source out-t-res map-zoom]
  (let [epoch-period (date/datetime->period out-t-res "2000-01-01")
        hansen-end-period (date/datetime->period out-t-res "2010-12-31")]
    (<- [?x ?y ?z ?p]
        (str->double ?lat :> ?latd)
        (str->double ?lon :> ?lond)
        (hansen-source ?lat ?lon _)
        (- hansen-end-period epoch-period :> ?p)
        (latlon->tile ?latd ?lond map-zoom :> ?x ?y ?z))))

(defn forma->cdm
  "Output x,y,z and period of first detection greater than threshold, per
   specification of common data model with Vizzuality.

  Arguments:
    est-map - A mapping that contains :est-start key with a datestring value
    forma-src - The FORMA source data tap
    thresh - The threshold number
    t-res - The temporal resolution as a string
    out-t-res - The output temporal resolution as a string
    map-zoom - The map zoom level"
  [est-map forma-src thresh t-res out-t-res map-zoom]
  (let [epoch-period (date/datetime->period out-t-res "2000-01-01")
        ts-start-period (date/datetime->period t-res (:est-start est-map))]
    (<- [?x ?y ?z ?p]
        (forma-src ?s-res ?mod-h ?mod-v ?s ?l ?prob-series)
        (o/clean-probs ?prob-series :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ ts-start-period ?first-hit-idx :> ?period)
        (date/convert-period-res t-res out-t-res ?period :> ?period-new-res)
        (- ?period-new-res epoch-period :> ?rp)
        (min-period ?rp :> ?p)
        (r/modis->latlon ?s-res ?mod-h ?mod-v ?s ?l :> ?lat ?lon)
        (latlon->tile ?lat ?lon map-zoom :> ?x ?y ?z))))
