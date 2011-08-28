(ns forma.hadoop.cluster
  (:use [clojure.string :only (join)]
        [clojure.contrib.command-line :only (with-command-line)]
        [pallet-hadoop.node :exclude (jobtracker-ip)]
        [pallet.crate.hadoop :only (hadoop-user)]
        
        [pallet.extensions :only (def-phase-fn phase)])
  (:require [pallet.execute :as execute]
            [forma.hadoop.environment :as env]
            [pallet.script :as script]
            [pallet.stevedore :as stevedore]    
            [pallet.action.exec-script :as exec-script]
            [pallet.resource.remote-directory :as rd]
            [pallet.resource.directory :as d]
            [pallet.resource.package :as package])
  (:import [backtype.hadoop ThriftSerialization]
           [cascading.tuple.hadoop BytesSerialization TupleSerialization]
           [org.apache.hadoop.io.serializer WritableSerialization JavaSerialization]))

;; ### Job Run

(def-phase-fn install-redd-configs
  "Takes pairs of strings -- the first should be a filename in the
  `reddconfig` bucket on s3, and the second should be the unpacking
  directory on the node."
  [& filename-localpath-pairs]
  (for [[remote local] (partition 2 filename-localpath-pairs)]
    (rd/remote-directory local
                         :url (str "https://reddconfig.s3.amazonaws.com/" remote)
                         :unpack :tar
                         :tar-options "xz"
                         :strip-components 2
                         :owner hadoop-user
                         :group hadoop-user)))

(def fw-path "/usr/local/fwtools")
(def native-path "/home/hadoop/native")

(def serializers
  (join "," (for [cls [ThriftSerialization BytesSerialization
                       TupleSerialization WritableSerialization
                       JavaSerialization]]
              (.getName cls))))

(def tokens
  (join "," [
             "130=forma.schema.IntArray"
             "131=forma.schema.DoubleArray"
             "132=forma.schema.FireTuple"
             "133=forma.schema.FireSeries"
             "134=forma.schema.FormaValue"
             "135=forma.schema.FormaNeighborValue"
             "136=forma.schema.FormaSeries"
             ]))

(def-phase-fn config-redd
  "This phase installs the two files that we need to make redd run
  with gdal! We also change the permissions on `/mnt` to allow for
  some heavy duty storage space. We do need some better documentation,
  here."
  []
  (d/directory "/mnt"
               :owner hadoop-user
               :group hadoop-user
               :mode "0755")
  (package/package "libhdf4-dev")
  (install-redd-configs
   "FWTools-linux-x86_64-4.0.0.tar.gz" fw-path
   "linuxnative.tar.gz" native-path))

(def-phase-fn raise-file-limits
  "Raises file descriptor limits on all machines to the specified
  limit."
  [lim]
  (let [command  #(join " " [hadoop-user % "nofile" 65000])
        path "/etc/security/limits.conf"
        pam-lims "session required  pam_limits.so"]
    (exec-script/exec-script
     ((echo ~(command "hard")) ">>" ~path))
    (exec-script/exec-script
     ((echo ~(command "soft")) ">>" ~path))
    (exec-script/exec-script
     ((echo ~pam-lims) ">>" "/etc/pam.d/common-session"))))

(defn mk-profile
  [mappers reducers spot-price ami hardware-id]
  {:map-tasks mappers
   :reduce-tasks reducers
   :image-id ami
   :hardware-id hardware-id
   :price spot-price})

(def cluster-profiles
  {"large"           (mk-profile 4 3 0.50 "us-east-1/ami-08f40561" "m1.large")
   "high-memory"     (mk-profile 30 27 1.50 "us-east-1/ami-08f40561" "m2.4xlarge")
   "cluster-compute" (mk-profile 22 16 0.80 "us-east-1/ami-1cad5275" "cc1.4xlarge")})

(defn forma-cluster
  "Generates a FORMA cluster with the supplied number of nodes. We
  pick that reduce capacity based on the recommended 1.2 times the
  number of tasks times number of nodes."
  [cluster-key nodecount]
  {:pre [(not (nil? cluster-key))]}
  (let [lib-path (str fw-path "/usr/lib")
        {:keys [map-tasks reduce-tasks image-id hardware-id price]}
        (cluster-profiles (or cluster-key "high-memory"))]
    (cluster-spec :private
                  {:jobtracker (node-group [:jobtracker :namenode])
                   :slaves     (slave-group nodecount)}
                  :base-machine-spec {:hardware-id hardware-id
                                      :image-id image-id
                                      :spot-price (float price)}
                  :base-props {:hadoop-env {:JAVA_LIBRARY_PATH native-path
                                            :LD_LIBRARY_PATH lib-path}
                               :hdfs-site {:dfs.data.dir "/mnt/dfs/data"
                                           :dfs.name.dir "/mnt/dfs/name"
                                           :dfs.datanode.max.xcievers 5096}
                               :core-site {:io.serializations serializers
                                           :cascading.serialization.tokens tokens
                                           :fs.s3n.awsAccessKeyId "AKIAJ56QWQ45GBJELGQA"
                                           :fs.s3n.awsSecretAccessKey
                                           "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"}
                               :mapred-site {:mapred.local.dir "/mnt/hadoop/mapred/local"
                                             :mapred.task.timeout 10000000
                                             :mapred.reduce.tasks (int (* reduce-tasks nodecount))
                                             :mapred.tasktracker.map.tasks.maximum map-tasks
                                             :mapred.tasktracker.reduce.tasks.maximum reduce-tasks
                                             :mapred.reduce.max.attempts 12
                                             :mapred.map.max.attempts 20
                                             :mapred.map.tasks.speculative.execution false
                                             :mapred.reduce.tasks.speculative.execution false
                                             :mapred.child.java.opts (str "-Djava.library.path="
                                                                          native-path
                                                                          " -Xms1024m -Xmx1024m")
                                             :mapred.child.env (str "LD_LIBRARY_PATH="
                                                                    lib-path)}})))

(defn jobtracker-ip
  [node-type]
  (let [{defs :nodedefs} (forma-cluster node-type 0)
        [jt-tag] (roles->tags [:jobtracker] defs)]
    (master-ip env/ec2-service jt-tag :public)))

(defn master-nodeset [node-type]
  (let [cluster (forma-cluster node-type 0)
        [jt-tag] (roles->tags [:jobtracker] (:nodedefs cluster))]
    (some #(when (= jt-tag (:group-name %)) %)
          (cluster->node-set cluster))))

(defn scp-uberjar
  [standalone-filepath dest-path ip-or-dns]
  (execute/local-script
   (scp ~standalone-filepath ~(str ip-or-dns ":" dest-path))))

;; Lein Run functions!

(defn print-jobtracker-ip
  [node-type]
  (println (if-let [ip (jobtracker-ip node-type)]
             (format "The current jobtracker IP is %s." ip)
             "Sorry, no cluster seems to be running at the moment."))
  (println "Hit Ctrl-C to exit."))

(defn create-cluster!
  [node-type node-count]
  (let [cluster (forma-cluster node-type node-count)]
    (do (println
         (format "Creating cluster of %s instances and %d nodes."
                 node-type node-count))
        (boot-cluster cluster
                      :compute env/ec2-service
                      :environment env/remote-env)
        (lift-cluster cluster
                      :phase (phase config-redd (raise-file-limits 90000))
                      :compute env/ec2-service
                      :environment env/remote-env)
        (start-cluster cluster
                       :compute env/ec2-service
                       :environment env/remote-env)
        (println "Cluster created!")
        (println "Hit Ctrl-C to exit."))))

(defn destroy-cluster!
  [node-type]
  (println "Destroying cluster.")
  (kill-cluster (forma-cluster node-type 0)
                :compute env/ec2-service
                :environment env/remote-env)
  (println "Cluster destroyed!")
  (println "Hit Ctrl-C to exit."))

(defn -main [& args]
  (with-command-line args
    "Provisioning tool for Hadoop clusters."
    [[start? "Start Cluster?"]
     [stop? "Shutdown Cluster?"]
     [jobtracker-ip? "Print jobtracker IP address?"]
     [type "Cluster name" "high-memory"]
     [size "Cluster size"]]
    (cond start? (if-not size
                   (println "Please define a cluster size.")
                   (create-cluster! type size))
          stop? (destroy-cluster! type)
          jobtracker-ip? (print-jobtracker-ip type)
          :else (println "Jeez, give me a fucking option, will you?"))))
