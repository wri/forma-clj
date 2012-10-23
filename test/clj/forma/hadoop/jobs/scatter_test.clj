(ns forma.hadoop.jobs.scatter-test
  (:use cascalog.api
        forma.hadoop.jobs.scatter
        [midje sweet cascalog]
        [forma.thrift :as thrift]))

(fact
  "Test for `map-round`"
  (map-round (thrift/TimeSeries* 0 3 [1.1 2.6 3.4 4.0])) => [0 [1 3 3 4]])

(fact
  "Test `static-tap`"
  (let [pixel-loc (thrift/ModisPixelLocation* "500" 28 8 0 0)
      dc-src [["pail-path" (thrift/DataChunk* "vcf" pixel-loc 25 "00")]]]
  (static-tap dc-src)) => (produces [["500" 28 8 0 0 25]]))

(fact
  "Test `adjusted-precl-tap`"
  (let [base-t-res "32"
        target-t-res "16"
        precl-src [["500" 28 8 0 0 360 [1.5 2.5 3.4 4.7]]]]
    (adjusted-precl-tap base-t-res target-t-res precl-src))
  => (produces [["500" 28 8 0 0 690 [2 2 3 3 3 4 5]]]))
