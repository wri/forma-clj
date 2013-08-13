(ns forma.hadoop.jobs.preprocess-test
  (:use cascalog.api
        [midje sweet cascalog]
        [forma.hadoop.jobs.preprocess]
        [forma.source.tilesets :only (tile-set)])
  (:require [cascalog.io :as io]
            [cascalog.ops :as c]))

(fact
  "Test `parse-locations`"
  (parse-locations :all) => (tile-set :all)
  (parse-locations [:IDN [12 8]]) => (tile-set :IDN [12 8])
  (parse-locations [[28 8]]) => (tile-set [28 8])
  (parse-locations ":all") => (tile-set :all)
  (parse-locations "[:IDN [12 8]]") => (tile-set :IDN [12 8])
  (parse-locations "[[28 8]]") => (tile-set [28 8]))

(fact
  (let [fire-str (str "-16.701,137.752,338.2,1.7,1.3,2012-11-04, 01:25,T,89,5.0       ,298.1,63\n"
                      "1.0,104.0,338.2,1.7,1.3,2012-11-04, 01:25,T,89,5.0,298.1,63\n")
        
        fire-path (.getPath (io/temp-dir "raw-fires"))
        out-path (.getPath (io/temp-dir "fires-out"))
        _ (spit (str fire-path "/fires.csv") fire-str)
        counter (comp count first ??- hfs-seqfile)]
    
    (PreprocessFire fire-path out-path "500" "16" "2000-11-01" :all)
    (counter out-path) => 2

    (PreprocessFire fire-path out-path "500" "16" "2000-11-01" [[28 8]])
    (counter out-path) => 1

    (PreprocessFire fire-path out-path "500" "16" "2000-11-01" [[28 8] [31 10]])
    (counter out-path) => 2))
