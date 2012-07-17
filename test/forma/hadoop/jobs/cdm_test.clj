(ns forma.hadoop.jobs.cdm-test
  "This namespace defines Midge tests for the forma.hadoop.jobs.cdm
namespace."
  (:use cascalog.api
        forma.hadoop.jobs.cdm
        [midje sweet cascalog])
  (:require [forma.testing :as t]
            [cascalog.ops :as c]))

(tabular
 (fact "Test hansen-latlon->cdm function.  A valid lat-lon should produce the
specified row; but an invalid lat-lon will not produce a row."
   (hansen-latlon->cdm ?src ?zoom ?tres) => (produces ?row))
 ?src ?zoom ?tres ?row 
 [["-87.65005229999997,41.850033,1"]] 16 "32" [[16811 24364 16 131]]
 [["2000,2000,1"]] 16 "32" [])

(fact "Check that `forma->cdm` produces the expected output, ready for
input into CartoDB table."
  (let [src (hfs-seqfile (t/dev-path "/testdata/output"))
        gadm-src (hfs-seqfile (t/dev-path "/testdata/gadm-path"))
        out-tap (forma->cdm src gadm-src 17 "16" "32" "2005-12-31" 50)]
    (c/first-n out-tap 3)
    => (produces [[102620 65277 17 71 "IDN" 0.7104166666666597 101.85574611194706]
                  [102622 65277 17 71 "IDN" 0.7104166666666597 101.85991309892155]
                  [102625 65277 17 71 "IDN" 0.7104166666666597 101.86824707287063]])))
