(ns leiningen.cluster
  (:use [leiningen.compile :only (eval-in-project)]))

(defn create-cluster
  [node-type node-count]
  `(do (println (str (format "Running create-cluster of type %s, with %d nodes."
                             ~node-type ~node-count)))
       (forma.hadoop.cluster/create-cluster ~node-type ~node-count)
       (println "Cluster created! Hit Control-C to exit.")))

(defn destroy-cluster []
  `(do (println "Running destroy-cluster.")
       (forma.hadoop.cluster/destroy-cluster)
       (println "Cluster destroyed! Hit Control-C to exit.")))

(defn execute-jar
  "TODO: Fill this out with code for uberjarring, uploading to
  jobtracker and running a command as the hadoop user."  [])

(defn cluster
  "Start or destroy a pallet-hadoop cluster."
  ([project]
     (println
      "Go ahead and supply a subtask. Options are create and destroy."))
  ([project subtask & args]
     (let [task-func (case subtask
                           "create" create-cluster
                           "destroy" destroy-cluster
                           "execute" execute-jar)]
       (eval-in-project project
                        (apply task-func args)
                        nil nil
                        `(do (println "Loading cluster namespace...")
                             (require 'forma.hadoop.cluster)
                             (println "Loaded!"))))))

;; Gotta figure out native deps in leiningen.
