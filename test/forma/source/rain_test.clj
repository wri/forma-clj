(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use cascalog.api
        midje.sweet
        [forma.static :only (static-datasets)])
  (:require [forma.testing :as t])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(def precl-path
  (t/dev-path "/testdata/PRECL/precl_mon_v1.0.lnx.2000.gri0.5m.gz"))

(fact (floats-for-step 0.5) => 1036800)

;; TODO: Finish.
(fact "big-floats test.")
(fact "rain-tuples test.")
(fact "unpack-rain test.")

(facts "to-datestring tests."
  (to-datestring "precl.2002.gri0.5m" 12) => "2002-12-01"
  (to-datestring "precl.YYYY.gri0.5m" 12) => (throws AssertionError))

;; TODO: Finish.
(fact "to-rows test.")
(fact "rain-values test.")
(fact "resample-rain test.")
(fact "rain-chunks test.")

;; TODO: Mod for rain. Used to be for MODIS.
;;
;; (fact "rain-chunks test. `rain-chunks` is bound to a subquery, which
;; is used as a source for the final count aggregator. We check that the
;; chunk-size makes sense for the supplied dataset.

;; Note that this only works when the file at hdf-path has a spatial
;; resolution of 1000m; otherwise, the relationship between chunk-size
;; and total chunks becomes off."
;;   (let [ascii-map (:precl static-datasets)
;;         file-tap (io/hfs-wholefile precl-path)
;;         subquery (rain-chunks "1000" ascii-map 24000 file-tap pix-tap)
;;         [[chunk-count]] (??<- [?count]
;;                               (subquery ?dataset ?s-res ?t-res ?tstring ?date ?chunkid ?chunk)
;;                               (c/count ?count))]
;;     chunk-count => 60))
