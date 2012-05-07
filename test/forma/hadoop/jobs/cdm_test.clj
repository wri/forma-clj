(ns forma.hadoop.jobs.cdm-test
  (:use cascalog.api
        forma.hadoop.jobs.cdm
        [midje sweet cascalog]))

(fact
  (let [est-map {:est-start "2005-12-31"}
        forma-src [["500" 28 8 1 1 [1 45 99]]]
        thresh 50
        t-res "16"
        out-t-res "32"]
        (convert-for-vizz est-map forma-src thresh t-res out-t-res) => (produces [[51254 30939 16 71]])))
