(ns forma.hadoop.jobs.load-tseries
  (:use cascalog.api
        [forma.trends :only (timeseries)]
        [forma.date-time :only (datetime->period)]
        [forma.source.modis :only (tile-position
                                   tilestring->hv)])
  (:require [forma.hadoop.io :as io])
  (:gen-class))

;; TODO: Make sure that this is really the right missing val. Probably not!
(def
  ^{:doc "Predicate macro aggregator that generates a timeseries, given
  `?chunk`, `?temporal-resolution` and `?date`. Currently only
  functions when `?chunk` is an instance of `forma.schema.DoubleArray`."}
  form-tseries
  (<- [?temporal-res ?date ?chunk :> ?pix-idx ?t-start ?t-end ?tseries]
      (datetime->period ?temporal-res ?date :> ?tperiod)
      (:sort ?tperiod)
      (timeseries [-9999] ?tperiod ?chunk :> ?pix-idx ?t-start ?t-end ?tseries)))

;; TODO: Get count-vals working for int arrays
(defn extract-tseries
  [chunk-source]
  (<- [?dataset ?s-res ?t-res ?tilestring ?chunk-size
       ?chunkid ?pix-idx ?t-start ?t-end ?tseries]
      (chunk-source ?dataset ?s-res ?t-res ?tilestring ?date ?chunkid ?chunk)
      (io/count-vals ?chunk :> ?chunk-size)
      (form-tseries ?t-res ?date ?chunk :> ?pix-idx ?t-start ?t-end ?tseries)))

(defn process-tseries
  "Given a source of timeseries, this subquery extracts the proper
  position information and outputs the timeseries tagged well."
  [tseries-source]
  (<- [?dataset ?s-res ?t-res ?tile-h ?tile-v ?sample ?line ?t-start ?t-end ?tseries]
      (tseries-source ?dataset ?s-res ?t-res ?tilestring
                      ?chunk-size ?chunkid ?pix-idx ?t-start ?t-end ?tseries)
      (tilestring->hv ?tilestring :> ?tile-h ?tile-v)
      (tile-position ?s-res ?chunk-size ?chunkid ?pix-idx :> ?sample ?line)))

;; To get the rain and ndvi series loaded, we ran
;;
;; hadoop jar jarpath forma.hadoop.jobs.load_tseries
;; s3n://redddata/ndvi/1000-32/*/*/ /timeseries/ndvi/
;;
;; hadoop jar jarpath forma.hadoop.jobs.load_tseries
;; s3n://redddata/precl/1000-32/*/*/ /timeseries/precl/

(defn -main
  [in-path output-path]
  (?- (hfs-seqfile output-path)
      (-> (hfs-seqfile in-path)
          extract-tseries
          process-tseries)))

;; This is the old one, that allows for the pieces. We'll reinstitute
;; it when I'm sure that the pieces get processed properly... not sure
;; how `map read-string` does.
;;
;; (defn -main
;;   "TODO: Docs.

;;   Sample usage:

;;       (-main s3n://redddata/ /timeseries/ \"ndvi\"
;;              \"1000-32\" [\"008006\" \"008009\"])"
;;   [base-input-path output-path & pieces]
;;   (let [pieces (map read-string pieces)
;;         chunk-source (apply io/chunk-tap base-input-path pieces)]
;;     (?- (hfs-seqfile output-path)
;;         (-> chunk-source
;;             extract-tseries
;;             process-tseries))))
