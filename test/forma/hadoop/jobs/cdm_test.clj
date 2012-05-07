(ns forma.hadoop.jobs.cdm-test
  (:use cascalog.api
        forma.hadoop.jobs.cdm
        [midje sweet cascalog]
        [forma.gfw.cdm :only (latlon->google-tile)]
        [forma.utils :only (positions)])
  (:require [forma.date-time :as date]
            [forma.postprocess.output :as o]
            [forma.reproject :as r]))

(comment
  (let [est-map {:est-start "2005-12-31"}
        forma-src [["500" 28 8 1 1 [1 2 50]]]
        thresh 50
        t-res "16"
        out-t-res "32"]
        (convert-for-vizz est-map forma-src thresh t-res out-t-res) => (produces [1, 1, 16, 137])))

(comment
  (let [est-map {:est-start "2005-12-31"}
        forma-src [["500" 28 8 1 1 [1 2 50]]]
        thresh 50
        t-res "16"
        out-t-res "32"
        epoch-period (date/datetime->period out-t-res "2000-01-01")
        ts-start-period (date/datetime->period t-res (:est-start est-map))
        z 16]
    (<- [?x ?y ?z ?p]
        (forma-src ?s-res ?mod-h ?mod-v ?s ?l ?prob-series)
        (o/clean-probs ?prob-series :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ ts-start-period ?first-hit-idx :> ?period)
        (date/convert-period-res t-res out-t-res ?period :> ?period-new-res)
        (- ?period-new-res epoch-period :> ?rp)
        (min-period ?rp :> ?p)
        (r/modis->latlon ?s-res ?mod-h ?mod-v ?s ?l :> ?lat ?lon)
        (latlon->google-tile ?lat ?lon z :> ?x ?y)
        (identity z :> ?z))))
