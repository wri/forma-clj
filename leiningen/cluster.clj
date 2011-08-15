(ns leiningen.cluster
  (:use [leiningen.compile :only (eval-in-project)]
        [leiningen.jar :only (get-jar-filename get-default-uberjar-name)]
        [leiningen.uberjar :only (uberjar)]))

(defn cluster-eval
  [project form]
  (eval-in-project project form nil nil
                   `(do (println "Loading cluster namespace...")
                        (require '[forma.hadoop.cluster :as c])
                        (println "Loaded!"))))

(defn create-cluster
  [project node-type node-count]
  (cluster-eval
   project
   `(do (println (str (format "Running create-cluster of type %s, with %d nodes."
                              ~node-type ~node-count)))
        (c/create-cluster ~node-type ~node-count)
        (println "Cluster created! Hit Control-C to exit."))))

(defn destroy-cluster [project]
  (cluster-eval
   project
   `(do (println "Running destroy-cluster.")
        (c/destroy-cluster)
        (println "Cluster destroyed! Hit Control-C to exit."))))

;; TODO: Implement jobtracker-ip. Can we get away with no node-set?
(defn execute-jar
  [project node-type & args]
  (let [uberjar-name (get-default-uberjar-name project)
        standalone-filename (get-jar-filename project uberjar-name)]
    (println (str "Uberjarring project to " standalone-filename))
    (uberjar project)
    (cluster-eval
     project
     `(do (let [jobtracker-ip# (c/jobtracker-ip (c/forma-cluster ~node-type 10))]
            (println "Pretending to run some shit.")
            (comment "scp uberjar up to jobtracker ip."))))))

(defn cluster
  "Start or destroy a pallet-hadoop cluster.

  Supported node-types are \"large\" and \"high-memory\"."
  ([project]
     (println "Go ahead and supply a subtask.")
     (println "Options are `create` and `destroy`."))
  ([project subtask & args]
     (let [task-func (case subtask
                           "create" create-cluster
                           "destroy" destroy-cluster
                           "execute" execute-jar)]
       (apply task-func project args))))

;; Gotta figure out native deps in leiningen.



