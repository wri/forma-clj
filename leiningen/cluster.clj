(ns leiningen.cluster
  (:use [leiningen.compile :only (eval-in-project)]
        [leiningen.jar :only (get-jar-filename get-default-uberjar-name)]
        [leiningen.uberjar :only (uberjar)]))

(defn cluster-eval
  [project form]
  (eval-in-project project form nil nil
                   `(do (println "Loading cluster namespace...")
                        (require 'forma.hadoop.cluster)
                        (println "Loaded!"))))

(defn create-cluster
  [project node-type node-count]
  (let [node-count (read-string node-count)]
    (cluster-eval
     project
     `(do (println (format "Creating cluster of %s instances and %d nodes."
                           ~node-type ~node-count))
          (forma.hadoop.cluster/create-cluster ~node-type ~node-count)
          (println "Cluster created!")
          (println "Hit Control-C to exit.")))))

(defn destroy-cluster
  [project node-type]
  (cluster-eval
   project
   `(do (println "Running destroy-cluster.")
        (forma.hadoop.cluster/destroy-cluster ~node-type)
        (println "Cluster destroyed!")
        (println "Hit Control-C to exit."))))

(defn show-jobtracker-ip
  [project node-type]
  (cluster-eval
   project
   `(do (if-let [jobtracker-ip# (forma.hadoop.cluster/jobtracker-ip ~node-type)]
          (println "The current jobtracker IP is" jobtracker-ip#)
          (println "Sorry, no cluster seems to be running at the moment."))
        (println "Hit Control-C to exit."))))

(defn execute-jar
  [project node-type & args]
  (let [uberjar-name (get-default-uberjar-name project)
        standalone-filename (get-jar-filename project uberjar-name)]
    (println (str "Uberjarring project to " standalone-filename))
    (uberjar project)
    (cluster-eval
     project
     `(do (let [jobtracker-ip# (forma.hadoop.cluster/jobtracker-ip ~node-type)]
            (println "Pretending to run some shit.")
            (comment "scp uberjar up to jobtracker ip.")
            (println "Hit Control-C to exit."))))))

(defn cluster
  "Start or destroy a pallet-hadoop cluster.

  Supported node-types are \"large\" and \"high-memory\"."
  ([project]
     (println "Go ahead and supply a subtask.")
     (println "Options are `create`, `destroy`, `show-ip` and `execute`."))
  ([project subtask & args]
     (let [task-func (case subtask
                           "create" create-cluster
                           "destroy" destroy-cluster
                           "jobtracker-ip" show-jobtracker-ip
                           "execute" execute-jar)]
       (apply task-func project args))))
