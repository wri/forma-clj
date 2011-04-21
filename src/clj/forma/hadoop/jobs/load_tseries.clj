(ns forma.hadoop.jobs.load-tseries
  (:use cascalog.api
        [forma.date-time :only (datetime->period)]
        [forma.source.modis :only (tilestring->hv)]
        [forma.hadoop.io :only (chunk-tap)]
        [forma.reproject :only (tile-position)]
        [forma.static :only (chunk-size)]
        [forma.trends :only (timeseries)])
  (:require [cascalog.ops :as c])
  (:gen-class))

(defn process-tseries
  [chunk-source]
  (<- [?dataset ?spatial-res ?temporal-res ?tile-h ?tile-v ?sample ?line ?pix-idx ?t-start ?t-end ?tseries]
      (chunk-source ?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?int-chunk)
      (datetime->period ?temporal-res ?date :> ?tperiod)
      (tilestring->hv ?tilestring :> ?tile-h ?tile-v)
      (:sort ?tperiod)
      (timeseries ?tperiod ?int-chunk :> ?pix-idx ?t-start ?t-end ?tseries)
      (tile-position ?spatial-res chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defn -main
  "TODO: Docs.

  Sample usage:

      (-main \"s3n://redddata/\" \"/timeseries/\" \"ndvi\"
             \"1000-32\" [\"008006\" \"008009\"])"
  [base-input-path output-path & pieces]
  (let [pieces (map read-string pieces)
        chunk-source (apply chunk-tap base-input-path pieces)]
    (?- (hfs-seqfile output-path)
        (process-tseries chunk-source))))
