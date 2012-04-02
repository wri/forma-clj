(ns forma.hadoop.jobs.forma-test
  (:use cascalog.api
        [midje sweet cascalog]
        [clojure.string :only (join)] forma.hadoop.jobs.forma)
  (:require [forma.schema :as schema]
            [forma.date-time :as date]
            [forma.hadoop.io :as io]
            [forma.hadoop.predicate :as p]
            [cascalog.ops :as c]))

(def some-map
  {:est-start "2005-12-01"
   :est-end "2011-04-01"
   :t-res "32"
   :neighbors 1
   :window-dims [600 600]
   :vcf-limit 25
   :long-block 15
   :window 5})

(defn run-dynamic
  "Test function that displays the output of the dynamic tap."
  []
  (?- (stdout)
      (-> (dynamic-cleaned-tap some-map
                       (hfs-seqfile "/Users/sritchie/Desktop/ndviseries1000/")
                       (hfs-seqfile "/Users/sritchie/Desktop/ndviseries1000/"))
          (c/first-n 10))))

;; FORMA, broken down into pieces. We're going to have sixteen sample
;; timeseries, to test the business with the neighbors.

(def dynamic-tuples
  (let [ts (schema/timeseries-value 370 [3 2 1])]
    (into [["1000" 13 9 610 611 ts ts ts]]
          (for [sample (range 4)
                line   (range 4)]
            ["1000" 13 9 sample line ts ts ts]))))

(def fire-values
  (let [keep? (complement #{[0 1] [0 2] [1 2] [3 2] [1 3] [2 3]})
        series (schema/timeseries-value 370 [(schema/fire-value 1 1 1 1)
                                             (schema/fire-value 0 1 1 1)
                                             (schema/fire-value 3 2 1 1)])]
    (into [["1000" 13 9 610 611 series]]
          (for [sample (range 4)
                line (range 4)
                :when (keep? [sample line])]
            ["1000" 13 9 sample line series]))))

(def outer-src
  (let [no-fire-3 (schema/forma-value nil 3 3 3 3)
        no-fire-2 (schema/forma-value nil 2 2 2 2)
        no-fire-1 (schema/forma-value nil 1 1 1 1)
        forma-3 (schema/forma-value (schema/fire-value 1 1 1 1) 3 3 3 3)
        forma-2 (schema/forma-value (schema/fire-value 0 1 1 1) 2 2 2 2)
        forma-1 (schema/forma-value (schema/fire-value 3 2 1 1) 1 1 1 1)]
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
     ["1000" 370 13 9 0 1 no-fire-3]
     ["1000" 371 13 9 0 1 no-fire-2]
     ["1000" 372 13 9 0 1 no-fire-1]
     ["1000" 370 13 9 1 1 forma-3]
     ["1000" 371 13 9 1 1 forma-2]
     ["1000" 372 13 9 1 1 forma-1]
     ["1000" 370 13 9 2 1 forma-3]
     ["1000" 371 13 9 2 1 forma-2]
     ["1000" 372 13 9 2 1 forma-1]
     ["1000" 370 13 9 3 1 forma-3]
     ["1000" 371 13 9 3 1 forma-2]
     ["1000" 372 13 9 3 1 forma-1]
     ["1000" 370 13 9 0 2 no-fire-3]
     ["1000" 371 13 9 0 2 no-fire-2]
     ["1000" 372 13 9 0 2 no-fire-1]
     ["1000" 370 13 9 1 2 no-fire-3]
     ["1000" 371 13 9 1 2 no-fire-2]
     ["1000" 372 13 9 1 2 no-fire-1]
     ["1000" 370 13 9 2 2 forma-3]
     ["1000" 371 13 9 2 2 forma-2]
     ["1000" 372 13 9 2 2 forma-1]
     ["1000" 370 13 9 3 2 no-fire-3]
     ["1000" 371 13 9 3 2 no-fire-2]
     ["1000" 372 13 9 3 2 no-fire-1]
     ["1000" 370 13 9 0 3 forma-3]
     ["1000" 371 13 9 0 3 forma-2]
     ["1000" 372 13 9 0 3 forma-1]
     ["1000" 370 13 9 1 3 no-fire-3]
     ["1000" 371 13 9 1 3 no-fire-2]
     ["1000" 372 13 9 1 3 no-fire-1]
     ["1000" 370 13 9 2 3 no-fire-3]
     ["1000" 371 13 9 2 3 no-fire-2]
     ["1000" 372 13 9 2 3 no-fire-1]
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
            (dynamic-cleaned-tap
             est-map
             (dynamic-filter 25 :n-src :reli-src :r-src :v-src)) => dynamic-tuples
             (fire-tap est-map :f-src) => fire-values))
  
  (fact?- "Can forma follow through?"
          forma-results
          (forma-query est-map :n-src :reli-src :r-src :v-src country-src :f-src)  
          (provided
            (forma-tap est-map :n-src :reli-src :r-src :v-src :f-src) => outer-tap)))

(fact
  "Uses simple timeseries to check cleaning operation"
  (let [t-res "16"
        ts-length 20
        est-map {:est-start (date/period->datetime t-res (- ts-length 10))
                 :est-end (date/period->datetime t-res ts-length)
                 :t-res t-res}
        ndvi [[1 (vec (range 8000 (+ 8000 ts-length)))]]
        precl [[1 (vec (range ts-length))]]
        reli [[1 [0 2 3 0 1 0 3 1 1 0 2 1 2 0 3 2 1 2 3 1]]]]
    (<- [?id ?series]
        (ndvi ?id ?ndvi)
        (reli ?id ?reli)
        (clean-timeseries-shell-long est-map 0 ?ndvi ?reli :> ?series)
        (:distinct false))) => (produces-some [[1 [8000.0 8001.5 8003.0 8003 8004 8005.0 8007.0 8007 8008 8009]]
                                               [1 [8000.0 8001.5 8003.0 8003 8004 8005.0 8007.0 8007 8008 8009.0 8011.0 8011.0 8013.0 8013.0 8014.5 8016.0 8016.0 8017.5 8019.0 8019]]]))


(fact
  "Uses simple timeseries to test cleaning query"
  (let [t-res "16"
        ts-length 20
        est-map {:est-start (date/period->datetime t-res (- ts-length 10))
                 :est-end (date/period->datetime t-res ts-length)
                 :t-res t-res}
        ndvi (vec (range 8000 (+ 8000 ts-length)))
        precl (vec (range ts-length))
        reli [0 2 3 0 1 0 3 1 1 0 2 1 2 0 3 2 1 2 3 1]
        dynamic-src [["500" 28 8 0 0 0 ndvi precl reli]]]
    (dynamic-clean-long est-map dynamic-src) => (produces-some [["500" 28 8 0 0 0 [8000.0 8001.5 8003.0 8003 8004 8005.0 8007.0 8007 8008 8009]]
                                                                ["500" 28 8 0 0 0 [8000.0 8001.5 8003.0 8003 8004 8005.0 8007.0 8007 8008 8009.0 8011.0 8011.0 8013.0 8013.0 8014.5 8016.0 8016.0 8017.5 8019.0 8019]]])))