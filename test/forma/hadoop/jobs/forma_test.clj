(ns forma.hadoop.jobs.forma-test
  (:use cascalog.api
        [midje sweet cascalog]
        [clojure.string :only (join)] forma.hadoop.jobs.forma)
  (:require [forma.schema :as schema]
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
      (-> (dynamic-tap some-map
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
            (dynamic-tap
             est-map
             (dynamic-filter 25 :n-src :reli-src :r-src :v-src)) => dynamic-tuples
            (fire-tap est-map :f-src) => fire-values))
  
  (fact?- "Can forma follow through?"
          forma-results
          (forma-query est-map :n-src :reli-src :r-src :v-src country-src :f-src)  
          (provided
            (forma-tap est-map :n-src :reli-src :r-src :v-src :f-src) => outer-tap)))

(def static-src
  ;; defined to match something on robin's computer
  [["500" 31 9 1480 583 -9999	57 40102 0]
   ["500" 32 9 2099 2256 -9999 57 40102 0]])

(comment
  (let [beta-src (hfs-seqfile "/Users/robin/Downloads/betas")
        dynamic-src (hfs-seqfile "/Users/robin/Downloads/dynamic")
        out (hfs-textline "/Users/robin/Downloads/text/apply" :sinkmode :replace)
        ;; trap-tap (hfs-seqfile "/Users/robin/Downloads/trap")
        ]
    (?- out (forma-estimate beta-src dynamic-src static-src ))))