(ns forma.hadoop.jobs.cdm
  (:use [cascalog.api]
        [forma.gfw.cdm :only (latlon->google-tile)]
        [forma.utils :only (positions)])
  (:require [forma.postprocess.output :as o]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [cascalog.ops :as c]))

(defn first-hit
  [thresh series]
  (first (positions (partial <= thresh) series)))

(defn convert-for-vizz
  "Output x,y,z and period of first detection greater than threshold, per specification of common data model with Vizzuality.

  Usage:
   (?- (hfs-seqfile \"s3n://formaresults/analysis/xyzperiod\")
     (get-first-hit (hfs-seqfile \"s3n://formaresults/finaloutput\") 50 \"16\"))"
  [est-map forma-src thresh t-res out-t-res]
  (let [epoch-period (date/datetime->period out-t-res "2000-01-01")
        ts-start-period (date/datetime->period t-res (:est-start est-map))
        z 16]
    (<- [?tx ?ty ?z ?relative-period]
        (forma-src ?s-res ?mod-h ?mod-v ?s ?l ?prob-series)
        (o/clean-probs ?prob-series :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ ts-start-period ?first-hit-idx :> ?period)
        (date/convert-period-res t-res out-t-res ?period :> ?period-new-res)
        (- ?period-new-res epoch-period :> ?relative-period)
        (r/modis->latlon ?s-res ?mod-h ?mod-v ?s ?l :> ?lat ?lon)
        (latlon->google-tile ?lat ?lon z :> ?tx ?ty)
        (identity z :> ?z))))