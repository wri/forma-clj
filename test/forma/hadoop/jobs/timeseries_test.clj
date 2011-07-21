(ns forma.hadoop.jobs.timeseries-test
  (:use cascalog.api
        forma.hadoop.jobs.timeseries
        [midje sweet cascalog])
  (:require [forma.hadoop.io :as io]
            [forma.date-time :as d]))

(defn test-chunks
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, int-struct>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [dataset periods chunk-size]
  (for [period (range periods)
        :let [date (d/period->datetime "32" period)
              location (io/chunk-location "1000" 8 6 0 chunk-size)
              chunk (-> (range chunk-size)
                        (io/int-struct)
                        (io/mk-data-value))]]
    ["path" (io/mk-chunk dataset "32" date location chunk)]))

(future-fact
 "Need to update this -- we want to check that the results of
this query don't contain -9999."
 (let [results (-> (concat (test-chunks "precl" 10 1200)
                           (test-chunks "ndvi" 10 1200))
                   (vec)
                   (extract-tseries -9999)
                   (??-)
                   (first))]
   (second results) =not=> "something about not containing -9999."))

;; TODO: Check this code for possible tests for the fires
;; timeseries. This comes from the main namespace, for the version of
;; FORMA that requires preloading data into HDFS for processsing.
;;
;; (defn run-test [path out-path]
;;   (?- (hfs-seqfile out-path)
;;       (->> (hfs-textline path)
;;            fire-source-daily
;;            (reproject-fires "1000")
;;            (aggregate-fires "32")
;;            (fire-series "32" "2000-11-01" "2011-04-01"))))

;; (def some-map
;;   {:est-start "2005-12-01"
;;    :est-end "2011-04-01"
;;    :t-res "32"
;;    :long-block 15
;;    :window 5})

;; (defn run-second [path]
;;   (let [src (hfs-seqfile path)]
;;     (?- (stdout)
;;         (-> (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?est-start ?count]
;;              (src ?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?start _ ?series)
;;              (adjust-fires some-map ?start ?series :> ?est-start ?fire-series)
;;              (io/count-vals ?fire-series :> ?count))
;;             (cascalog.ops/first-n 2)))))

;; (defn run-check []
;;   (let [src (hfs-seqfile "/Users/sritchie/Desktop/FireOutput/")]
;;     (?- (stdout)
;;         (-> (<- [?dataset ?m-res ?t-res ?mod-h ?mod-v ?sample ?line ?t-start ?t-end ?ct-series]
;;                 (src ?dataset ?m-res ?t-res
;;                      ?mod-h ?mod-v ?sample ?line ?t-start ?t-end ?ct-series))
;;             (cascalog.ops/first-n 2)))))
