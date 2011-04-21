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

(def
  ^{:doc "Predicate macro aggregator that extracts a timeseries, given
  `?chunk`, `?temporal-resolution` and `?date` dynamic vars."}
  extract-tseries
  (<- [?temporal-res ?date ?chunk :> ?pix-idx ?t-start ?t-end ?tseries]
      (datetime->period ?temporal-res ?date :> ?tperiod)
      (:sort ?tperiod)
      (timeseries ?tperiod ?int-chunk :> ?pix-idx ?t-start ?t-end ?tseries)))

(def
  ^{:doc "Predicate macro function to extract relevant spatial data
  from a given set of chunk fields."}
  extract-attrs
  (<- [?tilestring ?spatial-res ?chunk-size ?chunkid ?pix-idx :> ?tile-h ?tile-v ?sample ?line]
      (tilestring->hv ?tilestring :> ?tile-h ?tile-v)
      (tile-position ?spatial-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

(defn process-tseries
  "Given a source of chunks, this subquery generates timeseries with
  all relevant accompanying information."
  [chunk-source]
  (<- [?dataset ?spatial-res ?temporal-res ?tile-h ?tile-v ?sample ?line ?pix-idx ?t-start ?t-end ?tseries]
      (chunk-source ?dataset ?spatial-res ?temporal-res ?tilestring ?date ?chunkid ?int-chunk)
      (count ?int-chunk :> ?chunk-size)
      (extract-tseries ?temporal-res ?date ?int-chunk :> ?pix-idx ?t-start ?t-end ?tseries)
      (extract-attrs ?tilestring ?spatial-res ?chunkid ?pix-idx :> ?tile-h ?tile-v ?sample ?line)))

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
