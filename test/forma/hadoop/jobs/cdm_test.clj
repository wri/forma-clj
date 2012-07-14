(ns forma.hadoop.jobs.cdm-test
  "This namespace defines Midge tests for the forma.hadoop.jobs.cdm namespace."
  (:use cascalog.api
        forma.hadoop.jobs.cdm
        [midje sweet cascalog]))

(tabular
 (fact
   "Test hansen->cdm function."
   (hansen->cdm ?src ?zoom ?tres) => (produces ?row))
 ?src ?zoom ?tres ?row 
 [["-87.65005229999997" "41.850033" "1"]] 16 "32" [[16811 24364 16 131]]
 ;; Invalid lat/lon so there should be no result row.
 [["2000" "2000" "1"]] 16 "32" [])

(tabular
 (fact
   "Test forma->cdm function."  
   (forma->cdm ?src ?zoom ?tres ?tres-out ?start ?thresh) => (produces ?row))
 ?src ?zoom ?tres ?tres-out ?start ?thresh ?row
 [["500" 28 8 0 0 [1 49 90]]] 16 "16" "32" "2005-12-31" 50 [[51253 30938 16 71]])
