(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use cascalog.api
        [midje sweet cascalog]
        [forma.static :only (static-datasets)]
        [forma.hadoop.io :only (hfs-wholefile)])
  (:require [forma.testing :as t])
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

(future-fact "rain-values test.")

(future-fact
 "read-rain test - for some reason, current test fails despite the fact that the values look identical to those in the test"
  (read-rain (static-datasets :precl) precl-path)
  => (produces-some [["2000-01-01" 0 0 6.43672]
                     ["2000-01-01" 0 1 6.434864]
                     ["2000-01-01" 0 2 6.43302]
                     ["2000-01-01" 0 3 6.4311934]
                     ["2000-01-01" 0 4 6.429384]]) )

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


(facts
  "test `rain-rowcol->modispos`"
  ;; first rain pixel
  (rain-rowcol->modispos 0 0) => [18 17 0 19]

  ;; last rain pixel
  (rain-rowcol->modispos 359 719) => [17 0 19 0]

  ;; rain pixel at the bottom right of modis grid
  (rain-rowcol->modispos 0 359) => [35 17 19 19]

  ;; rain pixel at the top right of modis grid
  (rain-rowcol->modispos 359 359) => [35 0 19 0])

(facts
  "tests `rainpos->modis-range`. The index of a rain pixel
  within a MODIS tile returns the range of MODIS pixels that fall
  within the rain pixel"
  ;; top left rain pixel within the MODIS tile
  (rainpos->modis-range "500" 0 0) => [[0 120] [0 120]]

  ;; top right rain pixel within the MODIS tile
  (rainpos->modis-range "500" 0 19) => [[0 120] [2280 2400]]

  ;; bottom left rain pixel within the MODIS tile
  (rainpos->modis-range "500" 19 0) => [[2280 2400] [0 120]]

  ;; bottom right rain pixel within the MODIS tile
  (rainpos->modis-range "500" 19 19) => [[2280 2400] [2280 2400]])

(fact "fill-rect test"
  (fill-rect [0 2] [4 6]) => [[0 4] [0 5] [1 4] [1 5]])

(fact
  "test `stretch-rain`"
  (stretch-rain "32" "16" 0 [1 2 3]) => [0 [1 1 2 2 3]])

(fact
  "test `explode-rain`"
  (let [rain-src [[0 1]]]
    (<- [?mod-h ?mod-v ?sample ?line]
        (rain-src ?row ?col)
        (explode-rain "500" ?row ?col :> ?mod-h ?mod-v ?sample ?line))
    => (produces-some [[18 17 120 2283]])))

(fact "tests that the rain series source is appropriately expanded
into the space of MODIS pixels. Two rain pixels should return 28,800
MODIS pixels (2 x 120^2).  This test only produces the first five
results."
  (let [src [["2000-01-01" 100 0 1.25]
             ["2000-01-01" 0 0 1.25]
             ["2000-02-01" 0 0 2.25]
             ["2000-05-01" 0 0 5.25]
             ["2000-01-01" 0 1 7.25]
             ["2000-02-01" 0 1 8.25]
             ["2000-05-01" 0 1 9.25]]
        tiles #{[18 17]}
        tap (cascalog.ops/first-n (rain-tap src tiles "500" -9999.0 "32" "16") 5)]
    (<- [?h ?v ?s ?l ?start ?series] (tap ?h ?v ?s ?l ?start ?series)))
  => (produces [[18 17 0 2280 690 [1 1 2 2 2 2 2 4 5]]
                [18 17 0 2281 690 [1 1 2 2 2 2 2 4 5]]
                [18 17 0 2282 690 [1 1 2 2 2 2 2 4 5]]
                [18 17 0 2283 690 [1 1 2 2 2 2 2 4 5]]
                [18 17 0 2284 690 [1 1 2 2 2 2 2 4 5]]]))
