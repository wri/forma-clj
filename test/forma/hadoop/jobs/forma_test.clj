(ns forma.hadoop.jobs.forma-test
  (:use [forma.hadoop.jobs.forma] :reload)
  (:use cascalog.api
        [midje sweet cascalog]
        [clojure.string :only (join)]
        [forma.playground])
  (:require [forma.schema :as schema]
            [forma.thrift :as thrift]
            [forma.date-time :as date]
            [forma.trends.filter :as f]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [forma.reproject :as r]
            [cascalog.ops :as c])
  (:import [backtype.hadoop.pail Pail]))

(def some-map
  {:est-start "2005-12-01"
   :est-end "2011-04-01"
   :t-res "32"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 15
   :window 5})

;; FORMA, broken down into pieces. We're going to have sixteen sample
;; timeseries, to test the business with the neighbors.

(def dynamic-tuples
  (let [ts (schema/create-timeseries 370 [3 2 1])]
    (into [["1000" 13 9 610 611 ts ts ts]]
          (for [sample (range 4)
                line   (range 4)]
            ["1000" 13 9 sample line ts ts ts]))))

(def fire-values
  (let [keep? (complement #{[0 1] [0 2] [1 2] [3 2] [1 3] [2 3]})
        series (schema/create-timeseries 370 [(thrift/FireValue* 1 1 1 1)
                                             (thrift/FireValue* 0 1 1 1)
                                             (thrift/FireValue* 3 2 1 1)])]
    (into [["1000" 13 9 610 611 series]]
          (for [sample (range 4)
                line (range 4)
                :when (keep? [sample line])]
            ["1000" 13 9 sample line series]))))

(def outer-src
  (let [;; TODO: FireValue is required in the forma.thrift IDL.
        ;;no-fire-3 (thrift/FormaValue* nil 3 3 3 3)
        ;;no-fire-2 (thrift/FormaValue* nil 2 2 2 2)
        ;;no-fire-1 (thrift/FormaValue* nil 1 1 1 1)
        forma-3 (thrift/FormaValue* (thrift/FireValue* 1 1 1 1) 3.0 3.0 3.0 3.0)
        forma-2 (thrift/FormaValue* (thrift/FireValue* 0 1 1 1) 2.0 2.0 2.0 2.0)
        forma-1 (thrift/FormaValue* (thrift/FireValue* 3 2 1 1) 1.0 1.0 1.0 1.0)]
    [["1000" 370 13 9 0 0 forma-3]
     ["1000" 371 13 9 0 0 forma-2]
     ["1000" 372 13 9 0 0 forma-1]
     ["1000" 370 13 9 1 0 forma-3]
     ["1000" 371 13 9 1 0 forma-2]
     ["1000" 372 13 9 1 0 forma-1]
     ["1000" 370 13 9 2 0 forma-3]
     ["1000" 371 13 9 2 0 forma-2]
     ["1000" 372 13 9 2 0 forma-1]
     ["1000" 370 13 9 3 0 forma-3]
     ["1000" 371 13 9 3 0 forma-2]
     ["1000" 372 13 9 3 0 forma-1]
     ;;["1000" 370 13 9 0 1 no-fire-3]
     ;;["1000" 371 13 9 0 1 no-fire-2]
     ;;["1000" 372 13 9 0 1 no-fire-1]
     ["1000" 370 13 9 1 1 forma-3]
     ["1000" 371 13 9 1 1 forma-2]
     ["1000" 372 13 9 1 1 forma-1]
     ["1000" 370 13 9 2 1 forma-3]
     ["1000" 371 13 9 2 1 forma-2]
     ["1000" 372 13 9 2 1 forma-1]
     ["1000" 370 13 9 3 1 forma-3]
     ["1000" 371 13 9 3 1 forma-2]
     ["1000" 372 13 9 3 1 forma-1]
     ;;["1000" 370 13 9 0 2 no-fire-3]
     ;;["1000" 371 13 9 0 2 no-fire-2]
     ;;["1000" 372 13 9 0 2 no-fire-1]
     ;;["1000" 370 13 9 1 2 no-fire-3]
     ;;["1000" 371 13 9 1 2 no-fire-2]
     ;;["1000" 372 13 9 1 2 no-fire-1]
     ["1000" 370 13 9 2 2 forma-3]
     ["1000" 371 13 9 2 2 forma-2]
     ["1000" 372 13 9 2 2 forma-1]
     ;; ["1000" 370 13 9 3 2 no-fire-3]
     ;; ["1000" 371 13 9 3 2 no-fire-2]
     ;; ["1000" 372 13 9 3 2 no-fire-1]
     ["1000" 370 13 9 0 3 forma-3]
     ["1000" 371 13 9 0 3 forma-2]
     ["1000" 372 13 9 0 3 forma-1]
     ;; ["1000" 370 13 9 1 3 no-fire-3]
     ;; ["1000" 371 13 9 1 3 no-fire-2]
     ;; ["1000" 372 13 9 1 3 no-fire-1]
     ;; ["1000" 370 13 9 2 3 no-fire-3]
     ;; ["1000" 371 13 9 2 3 no-fire-2]
     ;; ["1000" 372 13 9 2 3 no-fire-1]
     ["1000" 370 13 9 3 3 forma-3]
     ["1000" 371 13 9 3 3 forma-2]
     ["1000" 372 13 9 3 3 forma-1]
     ["1000" 370 13 9 610 611 forma-3]
     ["1000" 371 13 9 610 611 forma-2]
     ["1000" 372 13 9 610 611 forma-1]
     ]))

(def forma-results
  [["1000" 1 "2000-12-01" 13 9 0 0
    (join \tab [0 1 1 1 2 2 2 0 2 2 2 3 2 2 2 2 2 2])]
   ["1000" 1 "2001-01-01" 13 9 0 0
    (join \tab [3 2 1 1 1 1 1 6 4 2 2 3 1 1 1 1 1 1])]
   ["1000" 1 "2000-11-01" 13 9 0 0
    (join \tab [1 1 1 1 3 3 3 2 2 2 2 3 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 0 1
    (join \tab [0 0 0 0 1 1 1 9 6 3 3 5 1 1 1 1 1 1])]
   ["1000" 1 "2000-11-01" 13 9 0 1
    (join \tab [0 0 0 0 3 3 3 3 3 3 3 5 3 3 3 3 3 3])]
   ["1000" 1 "2000-12-01" 13 9 0 1
    (join \tab [0 0 0 0 2 2 2 0 3 3 3 5 2 2 2 2 2 2])]
   ["1000" 1 "2000-12-01" 13 9 0 2
    (join \tab [0 0 0 0 2 2 2 0 2 2 2 5 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 0 2
    (join \tab [0 0 0 0 3 3 3 2 2 2 2 5 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 0 2
    (join \tab [0 0 0 0 1 1 1 6 4 2 2 5 1 1 1 1 1 1])]
   ["1000" 1 "2001-01-01" 13 9 0 3
    (join \tab [3 2 1 1 1 1 1 0 0 0 0 3 1 1 1 1 1 1])]
   ["1000" 1 "2000-11-01" 13 9 0 3
    (join \tab [1 1 1 1 3 3 3 0 0 0 0 3 3 3 3 3 3 3])]
   ["1000" 1 "2000-12-01" 13 9 0 3
    (join \tab [0 1 1 1 2 2 2 0 0 0 0 3 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 1 0
    (join \tab [1 1 1 1 3 3 3 4 4 4 4 5 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 1 0
    (join \tab [3 2 1 1 1 1 1 12 8 4 4 5 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 1 0
    (join \tab [0 1 1 1 2 2 2 0 4 4 4 5 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 1 1
    (join \tab [1 1 1 1 3 3 3 5 5 5 5 8 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 1 1
    (join \tab [3 2 1 1 1 1 1 15 10 5 5 8 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 1 1
    (join \tab [0 1 1 1 2 2 2 0 5 5 5 8 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 1 2
    (join \tab [0 0 0 0 3 3 3 4 4 4 4 8 3 3 3 3 3 3])]
   ["1000" 1 "2000-12-01" 13 9 1 2
    (join \tab [0 0 0 0 2 2 2 0 4 4 4 8 2 2 2 2 2 2])]
   ["1000" 1 "2001-01-01" 13 9 1 2
    (join \tab [0 0 0 0 1 1 1 12 8 4 4 8 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 1 3
    (join \tab [0 0 0 0 2 2 2 0 2 2 2 5 2 2 2 2 2 2])]
   ["1000" 1 "2001-01-01" 13 9 1 3
    (join \tab [0 0 0 0 1 1 1 6 4 2 2 5 1 1 1 1 1 1])]
   ["1000" 1 "2000-11-01" 13 9 1 3
    (join \tab [0 0 0 0 3 3 3 2 2 2 2 5 3 3 3 3 3 3])]
   ["1000" 1 "2000-12-01" 13 9 2 0
    (join \tab [0 1 1 1 2 2 2 0 5 5 5 5 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 2 0
    (join \tab [1 1 1 1 3 3 3 5 5 5 5 5 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 2 0
    (join \tab [3 2 1 1 1 1 1 15 10 5 5 5 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 2 1
    (join \tab [0 1 1 1 2 2 2 0 6 6 6 8 2 2 2 2 2 2])]
   ["1000" 1 "2001-01-01" 13 9 2 1
    (join \tab [3 2 1 1 1 1 1 18 12 6 6 8 1 1 1 1 1 1])]
   ["1000" 1 "2000-11-01" 13 9 2 1
    (join \tab [1 1 1 1 3 3 3 6 6 6 6 8 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 2 2
    (join \tab [3 2 1 1 1 1 1 12 8 4 4 8 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 2 2
    (join \tab [0 1 1 1 2 2 2 0 4 4 4 8 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 2 2
    (join \tab [1 1 1 1 3 3 3 4 4 4 4 8 3 3 3 3 3 3])]
   ["1000" 1 "2000-11-01" 13 9 2 3
    (join \tab [0 0 0 0 3 3 3 2 2 2 2 5 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 2 3
    (join \tab [0 0 0 0 1 1 1 6 4 2 2 5 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 2 3
    (join \tab [0 0 0 0 2 2 2 0 2 2 2 5 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 3 0
    (join \tab [1 1 1 1 3 3 3 3 3 3 3 3 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 3 0
    (join \tab [3 2 1 1 1 1 1 9 6 3 3 3 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 3 0
    (join \tab [0 1 1 1 2 2 2 0 3 3 3 3 2 2 2 2 2 2])]
   ["1000" 1 "2000-12-01" 13 9 3 1
    (join \tab [0 1 1 1 2 2 2 0 4 4 4 5 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 3 1
    (join \tab [1 1 1 1 3 3 3 4 4 4 4 5 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 3 1
    (join \tab [3 2 1 1 1 1 1 12 8 4 4 5 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 3 2
    (join \tab [0 0 0 0 2 2 2 0 4 4 4 5 2 2 2 2 2 2])]
   ["1000" 1 "2001-01-01" 13 9 3 2
    (join \tab [0 0 0 0 1 1 1 12 8 4 4 5 1 1 1 1 1 1])]
   ["1000" 1 "2000-11-01" 13 9 3 2
    (join \tab [0 0 0 0 3 3 3 4 4 4 4 5 3 3 3 3 3 3])]
   ["1000" 1 "2000-12-01" 13 9 3 3
    (join \tab [0 1 1 1 2 2 2 0 1 1 1 3 2 2 2 2 2 2])]
   ["1000" 1 "2000-11-01" 13 9 3 3
    (join \tab [1 1 1 1 3 3 3 1 1 1 1 3 3 3 3 3 3 3])]
   ["1000" 1 "2001-01-01" 13 9 3 3
    (join \tab [3 2 1 1 1 1 1 3 2 1 1 3 1 1 1 1 1 1])]
   ["1000" 1 "2000-12-01" 13 9 610 611
    (join \tab [0 1 1 1 2 2 2 0 0 0 0 0 0 0 0 0 0 0])]
   ["1000" 1 "2000-11-01" 13 9 610 611
    (join \tab [1 1 1 1 3 3 3 0 0 0 0 0 0 0 0 0 0 0])]
   ["1000" 1 "2001-01-01" 13 9 610 611
    (join \tab [3 2 1 1 1 1 1 0 0 0 0 0 0 0 0 0 0 0])]])

(comment
  (fact
  "Uses simple timeseries to test cleaning query"
  (let [t-res "16"
        ts-length 10
        est-map {:est-start (date/period->datetime t-res (- ts-length 5))
                 :est-end (date/period->datetime t-res ts-length)
                 :t-res t-res}
        ndvi [1 2 3 4 5 6 7 8 9 10] ;;(vec (range 8000 (+ 8000 ts-length)))
        precl (vec (range ts-length))
        reli [0 2 3 0 1 0 3 1 1 0]
        dynamic-src [["500" 28 8 0 0 0 ndvi precl reli]]]
    (dynamic-clean est-map dynamic-src) => (produces-some
                                            [["500" 28 8 0 0 0 [1.0 2.0 3.0 4 5]] ["500" 28 8 0 0 0 [1.0 2.0 3.0 4 5 6]]])))

  (let [est-map {:est-start "2005-12-01"
               :est-end "2011-04-01"
               :t-res "32"
               :neighbors 1
               :window-dims [600 600]
               :vcf-limit 25
               :long-block 15
               :window 5}
      outer-tap   (name-vars outer-src
                             ["?s-res" "?period"
                              "?mod-h" "?mod-v" "?sample" "?line"
                              "?forma-val"])
      country-src (into [["1000" 13 9 610  611 1]]
                        (for [sample (range 20)
                              line   (range 20)]
                          ["1000" 13 9 sample line 1]))]
  (fact?- "forma-tap tests."
          outer-src
          (forma-tap est-map :n-src :reli-src :r-src :v-src :f-src)
          (provided
            (analyze-trends
             est-map
             (dynamic-filter 25 :n-src :reli-src :r-src :v-src)) => dynamic-tuples
             (fire-tap est-map :f-src) => fire-values))
  
  (fact?- "Can forma follow through?"
          forma-results
          (forma-query est-map :n-src :reli-src :r-src :v-src country-src :f-src)  
          (provided
            (forma-tap est-map :n-src :reli-src :r-src :v-src :f-src) => outer-tap)))
)

(fact
  (let [model-ts [[1 [1 2 3]]]
        ts [[1 [1 2 3 4 5]]]]
    (<- [?id ?shorter]
        (model-ts ?id ?model-ts)
        (ts ?id ?ts)
        (f/shorten-ts ?model-ts ?ts :> ?shorter))) => (produces [[1 [1 2 3]]]))

(fact
  (let [ts-length 50
        est-map {:window 10 :long-block 30}
        ndvi (flatten [(range 25) (range 25 0 -1)])
        precl (flatten [(range 25 0 -1) (range 25)])
        clean-src [["500" 28 8 0 0 0 ndvi precl]]]
    (analyze-trends est-map clean-src)) => (produces-some [["500" 28 8 0 0 0 49 -0.46384872080089 -4.198030811863873E-16 -2.86153967746369 4.3841345603546955]]))

(fact
  (let [src [["500" 28 8 0 0 693 693 0.1 0.2 0.3 0.4 1]
             ["500" 28 8 0 0 693 694 0.11 0.12 0.13 0.14 1]
             ["500" 28 8 0 1 693 693 0.1 0.2 0.3 0.4 1]
             ["500" 28 8 0 1 693 694 0.11 0.12 0.13 0.14 1]]]
    (consolidate-trends src)) => (produces-some [["500" 28 8 0 0 693 [[693 694]] [[0.1 0.11]] [[0.2 0.12]] [[0.3 0.13]] [[0.4 0.14]]]]))
