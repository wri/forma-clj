(ns forma.hadoop.jobs.run-trends
  (:use cascalog.api)
  (:require [cascalog.ops :as c]))

(def ndvi-file
  "/Users/sritchie/Dropbox/N_drive/misc/ndvi_2808_0_59.csv")

(defn split
  [line]
  (int-array (map #(Float. %)
                  (.split line "\\s+"))))

#_(defn count-tseries [path]
  (let [source (hfs-textline path)]
    (?<- (stdout) [?count]
         (source ?textline)
         (split ?textline :> ?t-series)
         (whizbang ?t-series :> ?coef ?t-test))))
