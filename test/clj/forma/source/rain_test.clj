(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use cascalog.api
        [midje sweet cascalog]
        [forma.static :only (static-datasets)])
  (:require [forma.testing :as t])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(def precl-path
  (t/dev-path "/testdata/PRECL/precl_mon_v1.0.lnx.2000.gri0.5m.gz"))

(fact (floats-for-step 0.5) => 1036800)

(future-fact "rain-tuples test.")
(future-fact "unpack-rain test.")

(facts "to-datestring tests."
  (to-datestring "precl.2002.gri0.5m" 12) => "2002-12-01"
  (to-datestring "precl.YYYY.gri0.5m" 12) => (throws AssertionError))

(future-fact "to-rows test.")
(future-fact "rain-values test.")
(future-fact "rain-chunks test.")

(fact
  "Test resample-rain Cascalog query.
  
  The resample-rain query takes a tap that emits rain tuples and a tap that
  emits MODIS pixels. It emits the same rain tuples projected into MODIS
  pixel coordinates."
  (let [tile-seq #{[8 6]}
        file-tap nil 
        test-rain-data [["2000-01-01" 239 489 100]] 
        pix-tap [[8 6 0 0]]
        ascii-map {:corner [0 -90] :travel [+ +] :step 0.5 :nodata -999}
        m-res "500"]
    (resample-rain m-res ascii-map file-tap pix-tap test-rain-data)
    => (produces [["precl" "500" "32" "2000-01-01" 8 6 0 0 100.0]])))

;; TODO: Mod for rain. Used to be for MODIS.
;;
;; (fact "rain-chunks test. `rain-chunks` is bound to a subquery, which
;; is used as a source for the final count aggregator. We check that the
;; chunk-size makes sense for the supplied dataset.

;; Note that this only works when the file at hdf-path has a spatial
;; resolution of 1000m; otherwise, the relationship between chunk-size
;; and total chunks becomes off."
;;
;; (let [ascii-map (:precl static-datasets)
;;       file-tap (io/hfs-wholefile precl-path)
;;       subquery (rain-chunks "1000" ascii-map 24000 file-tap pix-tap)]
;;   (future-fact?<- [[60]] [?count]
;;            (subquery ?dataset ?s-res ?t-res ?tstring ?date ?chunkid ?chunk)
;;            (c/count ?count)))
