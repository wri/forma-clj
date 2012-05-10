(ns forma.hadoop.jobs.cdm-test
  "This namespace defines Midge tests for the forma.hadoop.jobs.cdm namespace."
  (:use cascalog.api
        forma.hadoop.jobs.cdm
        [midje sweet cascalog]))

(tabular
 (fact
   "Runs a Cascalog query using a local source tap."  
   (let [source-tap [["500" 28 8 0 0 [1 49 90]]]]
     (forma->cdm {:est-start "2005-12-31"} source-tap 50 "16" "32" ?map-zoom))
   => (produces ?results))
 ?map-zoom ?results
 16 [[51253 30938 16 71]]
 1 [[1 0 1 71]])


