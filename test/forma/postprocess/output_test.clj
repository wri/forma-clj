(ns forma.postprocess.output_test
  (:use [forma.postprocess.output] :reload)
  (:use [midje sweet cascalog]
        [cascalog.api]
        [midje.cascalog]
        [forma.date-time :only (period->datetime datetime->period)])
  (:require [forma.schema :as schema]
            [cascalog.ops :as c]))

(def output-map
  [{:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 5
    :sample 10
    :prob-series (schema/timeseries-value
                  (datetime->period "16" "2005-12-31")
                  [0.1 0.6 0.1 0.7 0.9])
    :tres        "16"
    :sres        "500"
    :hansen      0}
   {:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 5
    :sample 9
    :prob-series (schema/timeseries-value
                  (datetime->period "16" "2005-12-31")
                  [0.1 0.9 0.1 0.1 0.1])
    :tres        "16"
    :sres        "500"
    :hansen      1}
   {:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 4
    :sample 10
    :prob-series (schema/timeseries-value
                  (datetime->period "16" "2005-12-31")
                  [0.1 0.4 0.2 0.7 0.9])
    :tres        "16"
    :sres        "500"
    :hansen      0}
   {:cntry       "IDN"
    :admin       23456
    :modh 28
    :modv 7
    :line 5
    :sample 11
    :prob-series (schema/timeseries-value
                  (datetime->period "16" "2005-12-31")
                  [0.1 0.6 0.45 0.65 0.9])
    :tres         "16"
    :sres        "500"
    :hansen       1}
   {:cntry       "MYS"
    :admin       12345
    :modh 28
    :modv 7
    :line 4
    :sample 9
    :prob-series (schema/timeseries-value
                  (datetime->period "16" "2005-12-31")
                  [0.1 0.4 0.1 0.7 0.9])
    :tres        "16"
    :sres        "500"
    :hansen      1}])

(fact
  (convert-to-latlon (first output-map)) => [19.97708333333333 106.44884696799383])

(fact
  "Make sure a time series actually becomes monotonically increasing"
  (let [series [4 3 2 5 6 4]]
    (mono-inc series)) => [[4 4 4 5 6 6]])
(fact
  (let [ts (schema/timeseries-value 0 [1 2 3])]
    (mk-date-header "16" "p" ts)) => ["p19700101" "p19700117" "p19700202"])

(fact
  (backward-looking-mavg 3 [1 2 3 4]) => [[1.0 1.5 2.0 3.0]])

(fact
  (let [ts (schema/timeseries-value 0 [1 2 3])
        header (mk-date-header "16" "p" ts)]
    (csv-ify header)) => "p19700101,p19700117,p19700202")

(fact
  (let [ts (schema/timeseries-value 0 [1 2 3])]
    (mk-header "16" "p" ts "modh" "modv" "sample" "line" "lat" "lon" "gadm" "hansen")) => "modh,modv,sample,line,lat,lon,gadm,hansen,p19700101,p19700117,p19700202")

(fact
  (<- [?modh]
      (output-map ?out-m)
      (get-val ?out-m :modh :> ?modh)) => (produces [[28]]))

(fact
  (<- [?sum]
      (output-map ?out-m)
      (get-val ?out-m :sample :> ?val)
      (c/sum ?val :> ?sum)) => (produces-some [[49]]))

(fact
  (<- [?val]
      (output-map ?out-m)
      (get-val ?out-m :sample :> ?val)) => (produces-some [[9] [10] [11]]))

(fact
  (<- [?modh]
      (output-map ?out-m)
      (get-val ?out-m :modh :> ?modh)) => (produces-some [[28]]))

(fact
  (<- [?admin-mean]
      (output-map ?out-m)
      (get-val ?out-m :admin :> ?admin)
      (c/avg ?admin :> ?admin-mean)) => (produces-some [[21233.8]]))

(fact
  (<- [?modh ?modv]
      (output-map ?out-m)
      (get-val ?out-m :modh :modv :> ?modh ?modv)) => (produces-some [[28 7]]))

(fact
  (make-csv output-map 3) => (produces-some [["28,7,10,4,19.9812500000,106.4516613766,0.1,0.25,0.25,0.43333334,0.6"]]))
