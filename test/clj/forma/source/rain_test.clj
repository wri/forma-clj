(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use cascalog.api
        [midje sweet cascalog]
        [forma.static :only (static-datasets)]
        [clojure.contrib.math :only (floor)]
        [clojure.contrib.combinatorics :only (cartesian-product)])
  (:require [forma.testing :as t]
            [forma.hadoop.io :as io]
            [forma.reproject :as r]
            [forma.matrix.utils :as mu])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(def precl-path
  (t/dev-path "/testdata/PRECL/precl_mon_v1.0.lnx.2000.gri0.5m.gz"))

(fact (floats-for-step 0.5) => 1036800)

;; (future-fact "rain-tuples test.")
;; (future-fact "unpack-rain test.")

(facts "to-datestring tests."
  (to-datestring "precl.2002.gri0.5m" 12) => "2002-12-01"
  (to-datestring "precl.YYYY.gri0.5m" 12) => (throws AssertionError))

(fact
  "Test `to-rows`."
  (let [step 0.5
      src [[(vec (range (* 2 720)))]]]
  (<- [?row ?row-data]
      (src ?rain-data)
      (to-rows [step] ?rain-data :> ?row ?row-data))) => (produces-some [[0 (vec (range 720))]]))

(facts
  "Test that `all-nodata?` correctly identifies collections with no valid values"
  (all-nodata? -999 [1.0 2.0 -999.0]) => false
  (all-nodata? -999 [1.0 2.0 3.0]) => false
  (all-nodata? -999 [-999.0 -999.0 -999.0]) => true)

;; (future-fact "rain-values test.")
;; (future-fact "rain-chunks test.")

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


;; (defn tester []
;;   (let [ascii-map {:corner [0 -90] :travel [+ +] :step 0.5 :nodata -999}
;;         src (io/hfs-wholefile precl-path)
;;         a (rain-values (:step ascii-map) (:nodata ascii-map) src)]
;;     (??<- [?date ?row ?col ?val]
;;          (a ?date ?row ?col ?val))))

(facts "rain position to modis tile position tests"
  ;; first rain pixel
  (rain-rowcol->modispos 0 0) => [18 17 0 19]

  ;; last rain pixel
  (rain-rowcol->modispos 359 719) => [17 0 19 0]

  ;; rain pixel at the bottom right of modis grid
  (rain-rowcol->modispos 0 359) => [35 17 19 19]

  ;; rain pixel at the top right of modis grid
  (rain-rowcol->modispos 359 359) => [35 0 19 0])

(facts "tests that the index of a rain pixel within a MODIS tile
returns the range of MODIS pixels that fall within the rain pixel"
  ;; top left rain pixel within the MODIS tile
  (rainpos->modis-range "500" 0 0) => [[0 120] [0 120]]

  ;; top right rain pixel within the MODIS tile
  (rainpos->modis-range "500" 0 19) => [[0 120] [2280 2400]]

  ;; bottom left rain pixel within the MODIS tile
  (rainpos->modis-range "500" 19 0) => [[2280 2400] [0 120]]

  ;; bottom right rain pixel within the MODIS tile
  (rainpos->modis-range "500" 19 19) => [[2280 2400] [2280 2400]])

(facts "fill-rect test"
  (fill-rect [0 2] [4 6]) => [[0 4] [0 5] [1 4] [1 5]])
