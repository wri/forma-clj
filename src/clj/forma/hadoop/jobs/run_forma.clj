(ns forma.hadoop.jobs.run-forma
  (:use cascalog.api)
  (:require [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.trends.analysis :as a])
  ;; (:import [forma.schema FormaValue])
  )

;; ;; TODO: FINISH
;; (defn adjust
;;   "Appropriately truncates the incoming timeseries, and outputs a new
;;   start and both truncated series."
;;   [start-a series-a start-b series-b])

;; ;; TODO: FINISH
;; (defn adjust-fires
;;   "Returns the section of fires data found appropriate based on the
;;   information in the estimation parameter map."
;;   [est-map f-start f-series])

;; ;; TODO: FINISH
;; ;; TODO: CREATE STRUCT
;; (defn forma-val
;;   [fire short long t-stat]
;;   (FormaValue. fire short long t-stat))

;; (defn forma-schema
;;   "Accepts a number of timeseries of equal length and starting
;;   position, and converts the first entry in each timeseries to a
;;   `FormaValue`, for all first values and on up the sequence. Series
;;   must be supplied in the order specified by the arguments for
;;   `forma-val`."
;;   [& in-series]
;;   (apply map forma-val in-series))

;; ;; TODO: ASK DAN
;; (def some-map
;;   {:est-start 1
;;    :est-end 1
;;    :t-res "32"
;;    :long-block 15
;;    :window 5})

;; ;; Remove the whole idea of "training period" from the
;; ;; forma.trends.analysis stuff.

;; (defn dynamic-stats
;;   [est-map & countries]
;;   (let [tiles (map tile-set countries)
;;         ndvi-tap (hfs-seqfile "/path/to/ndviseries")
;;         precl-tap (hfs-seqfile "/path/to/preclseries")
;;         est-start (:est-start est-map)]
;;     (<- [?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?period ?forma-val]
;;         (ndvi-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?n-start _ !n-series)
;;         (precl-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?p-start _ !p-series)
;;         (fire-tap _ ?s-res ?t-res ?mod-h ?mod-v ?sample ?line ?f-start _ !f-series)
;;         (adjust ?p-start !p-series ?n-start !n-series :> ?start ?precl-series ?ndvi-series)
;;         (adjust-fires est-map ?f-start !f-series :> ?est-start ?fire-series)
;;         (a/short-trend-shell est-map ?start ?ndvi-series :> ?est-start ?short-series)
;;         (a/long-trend-shell est-map ?start ?ndvi-series ?precl-series :> ?est-start ?long-series ?t-stat-series)
;;         (forma-schema ?fire-series ?short-series ?long-series ?t-stat-series :> ?forma-series)
;;         (p/struct-index ?est-start ?forma-series :> ?period ?forma-val))))

;; (defn tester []
;;   (-> (dynamic-stats some-map :IDN :MYS)
;;       (p/sparse-windower ["?sample" "?line"] [600 600] "?forma-val" nil)))

;; ;; Next, we need to actually walk those damned windows!

;; (defn -main []
;;   )
