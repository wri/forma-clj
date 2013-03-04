(ns forma.hadoop.jobs.scatter-test
  (:use cascalog.api
        forma.hadoop.jobs.scatter
        [midje sweet cascalog]
        [forma.thrift :as thrift]
        [forma.hadoop.pail :only (to-pail)]))

(fact
  "Test `pail-tap`"
  (let [s-res "500"
        t-res "00"
        pixel-loc (thrift/ModisPixelLocation* s-res 28 8 0 0)
        data-chunk (thrift/DataChunk* "vcf" pixel-loc 25 t-res)
        pail-path "/tmp/pail-tap-test/"]
    (to-pail pail-path [[data-chunk]])
    (pail-tap pail-path "vcf" s-res t-res) => (produces-some [["" data-chunk]])))
