(ns forma.hadoop.jobs.run-trends
  (:use cascalog.api
        [forma.trends.analysis :only (whizbang)])
  (:require [cascalog.ops :as c]))

(defn split
  [line]
  [(vec (map #(Float. %) (drop 4 (.split line "\\s+"))))])

(defn prepare-seqfile
  [input-path output-path]
  (let [source (hfs-textline input-path)]
    (?<- (hfs-seqfile output-path)
         [?t-series]
         (source ?textline)
         (split ?textline :> ?t-series))))

(defn count-tseries
  [seqfile-path]
  (let [source (hfs-seqfile seqfile-path)]
    (?<- (stdout) [?coeff ?t-stat]
         (source ?t-series)
         (whizbang ?t-series :> ?coeff ?t-stat))))
