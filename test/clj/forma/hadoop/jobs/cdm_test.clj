(ns forma.hadoop.jobs.cdm-test
  "This namespace defines Midge tests for the forma.hadoop.jobs.cdm
namespace."
  (:use [midje sweet]
        forma.hadoop.jobs.cdm))

;; TODO: Fix this. See: https://github.com/reddmetrics/forma-clj/issues/160
;; (tabular
;;  (fact "Test hansen-latlon->cdm function.  A valid lat-lon should produce the
;; specified row; but an invalid lat-lon will not produce a row."
;;    (hansen-latlon->cdm ?src ?zoom ?tres) => (produces ?row))
;;  ?src ?zoom ?tres ?row 
;;  [["-87.65005229999997,41.850033,1"]] 16 "32" [[16811 24364 16 131]]
;;  [["2000,2000,1"]] 16 "32" [])

(fact "Test that the output of `forma->cdm` correspond to the proper
common data model for upload into CartoDB.  Note that only the pixels
with ?sample equal to 422 and 423 are returned, since they are the
only pixels with smoothed probabilities that exceed the threshold of
`50`. Note also that the period of first-alert is later for 422 than
for 423, since the probabilities are lower -- taking more time to
register as an alert, given the smoothing of the raw probability
series."
  (let [raw-output [["500" 28 8 420 2182 [0.0002 0.002 0.02 0.2 0.2 0.2]]
                    ["500" 28 8 421 2182 [0.0004 0.004 0.04 0.4 0.4 0.4]]
                    ["500" 28 8 422 2182 [0.0006 0.006 0.06 0.6 0.6 0.6]]
                    ["500" 28 8 423 2182 [0.0008 0.008 0.08 0.8 0.8 0.8]]]
        gadm-src   [["500" 28 8 420 2182 15614]
                    ["500" 28 8 421 2182 15614]
                    ["500" 28 8 422 2182 15614]
                    ["500" 28 8 423 2182 15614]]]
    (forma->cdm raw-output gadm-src 17 "16" "32" "2005-12-31" 50)
    => (produces [[102590 65206 17 74 "IDN" 0.9062499999999973 101.77314715106174]
                  [102591 65206 17 73 "IDN" 0.9062499999999973 101.7773143389889]])))
