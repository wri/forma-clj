(ns forma.hadoop.jobs.postprocess-test
  "This namespace defines Midge tests for the forma.hadoop.jobs.cdm
namespace."
  (:use [midje sweet]
        forma.hadoop.jobs.postprocess
        cascalog.api
        midje.cascalog))

;; TODO: Fix this. See: https://github.com/reddmetrics/forma-clj/issues/160
;; (tabular
;;  (fact "Test hansen-latlon->cdm function.  A valid lat-lon should produce the
;; specified row; but an invalid lat-lon will not produce a row."
;;    (hansen-latlon->cdm ?src ?zoom ?tres) => (produces ?row))
;;  ?src ?zoom ?tres ?row 
;;  [["-87.65005229999997,41.850033,1"]] 16 "32" [[16811 24364 16 131]]
;;  [["2000,2000,1"]] 16 "32" [])

(facts "Test that the output of `forma->website` correspond to the proper
common data model for upload into CartoDB.  Note that only the pixels
with ?sample equal to 422 and 423 are returned, since they are the
only pixels with smoothed probabilities that exceed the threshold of
`50`. Note also that the period of first-alert is later for 422 than
for 423, since the probabilities are lower -- taking more time to
register as an alert, given the smoothing of the raw probability
series."
  (let [nodata -9999.0
        raw-output [["500" 28 8 420 2182 827 [0.0002 0.002 0.02 0.2 0.2 0.2] 88500 40158]
                    ["500" 28 8 421 2182 827 [0.0004 0.004 0.04 0.4 0.4 0.4] 88500 40158]
                    ["500" 28 8 422 2182 827 [0.0006 0.006 0.06 0.6 0.6 0.6] 88500 40158]
                    ["500" 28 8 423 2182 827 [0.0008 0.008 0.08 0.8 0.8 0.8] 88500 40158]]]
    (forma->website raw-output nodata 17 14 "16" "32" "2005-12-19" 50)
    => (produces [[12823N 8150N 14 "\"{73,74}\"" "\"{1,1}\""]
                  [25647N 16301N 15 "\"{73,74}\"" "\"{1,1}\""]
                  [51295N 32603 16 "\"{73,74}\"" "\"{1,1}\""]
                  [102590 65206 17 "\"{74}\"" "\"{1}\""]
                  [102591 65206 17 "\"{73}\"" "\"{1}\""]])

    (let [disc-map {40158 0.8}]
      (forma->website raw-output nodata 17 14 "16" "32" "2005-12-19" 50 disc-map)
      => (produces [[12823N 8150N 14 "\"{74}\"" "\"{1}\""]
                    [25647N 16301N 15 "\"{74}\"" "\"{1}\""]
                    [51295N 32603 16 "\"{74}\"" "\"{1}\""]
                    [102591 65206 17 "\"{74}\"" "\"{1}\""]]))))

(fact "Test `probs->country-stats."
  (let [thresh 50
        t-res "16"
        t-res-out "32"
        nodata -9999.0
        src [["500" 28 8 0 0 827 [0.2 0.5 0.8 0.9] 88500]
             ["500" 28 8 0 1 827 [0.1 0.1 0.1 0.1] 88500]
             ["500" 28 8 0 2 827 [0.1 0.1 0.5 0.9] 88501]]]
    (probs->country-stats thresh nodata t-res t-res-out src))
  => (produces [["IDN" 72 "2006-01-01" 1]
                ["IDN" 73 "2006-02-01" 1]]))

(fact "Test `forma->blue-raster`."
  (let [nodata -9999.0
        src [["500" 28 8 0 0 827 [0.4 0.5 0.6] 88500 2]]
        static-src [["500" 28 8 0 0 50 -1 2 1 4]]]
    (forma->blue-raster src static-src nodata)
    => (produces [["500" 28 8 0 0 9.99791666666666 101.54412568476158 "IDN"
                   50 88500 2 1 [40 45 50]]])

    (let [disc-map {2 0.5}]
      (forma->blue-raster src static-src nodata disc-map))
    => (produces [["500" 28 8 0 0 9.99791666666666 101.54412568476158 "IDN" 50 88500 2 1 [20 22 25]]])))

(fact "Test `forma-download`."
  (let [thresh 50
      t-res "16"
      nodata -9999.0
      src [["500" 28 8 0 0 827 [0.1 0.9 0.75] 88500 2]]]
    (forma-download src thresh t-res nodata) => (produces [["9.99791667" "101.54412568" "IDN" 88500 "2006-01-01"]])

    (let [disc-map {2 0.9}]
      (forma-download src thresh t-res nodata disc-map))
    => (produces [["9.99791667" "101.54412568" "IDN" 88500 "2006-01-17"]])))

(future-fact "Test `forma-website`. Need real data to make test more meaningful.")
