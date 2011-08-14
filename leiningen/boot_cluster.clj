(ns leiningen.boot-cluster
  (:use forma.hadoop.cluster :only (create-cluster)))

(defn boot-cluster [project & args]
  (leiningen.uberjar project)
  (println "Should be booting cluster but I'm SLACKING!!"))
