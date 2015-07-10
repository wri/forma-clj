(defproject forma "2.0.0-SNAPSHOT"
  :description "[FORMA](http://goo.gl/4YTBw) gone Functional."
  :min-lein-version "2.5.0"
  :repositories {"conjars" "http://conjars.org/repo/"}
  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :java-source-paths ["src/jvm"]
  :resources-paths ["resources"]
  :dev-resources-paths ["dev"]
  :jvm-opts ["-XX:+UseConcMarkSweepGC" "-Xms1024M" "-Xmx1048M" "-server"]
  :main nil
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/data.csv "0.1.2"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 [incanter/incanter-core "1.5.6"]
                 [clj-time "0.7.0"]
                 [forma/gdal "1.8.0"]
                 [forma/jblas "1.2.1"]
                 [cascalog "2.1.1"]
                 [cascalog/cascalog-checkpoint "2.1.1"]
                 [com.backtype/dfs-datastores "1.3.6"]
                 [com.backtype/dfs-datastores-cascading "1.3.6"]
                 [org.apache.thrift/libthrift "0.8.0"
                  :exclusions [org.slf4j/slf4j-api]]
                 [net.lingala.zip4j/zip4j "1.3.1"]]
  :aot [forma.hadoop.pail, forma.schema, #"forma.hadoop.jobs.*"]
  :profiles {:provided {:dependencies [[org.apache.hadoop/hadoop-core "1.2.1"]]}
             :dev {:dependencies [[cascalog/midje-cascalog "2.1.1"]
                                  [incanter/incanter-charts "1.3.0"]]
                   :plugins [[lein-midje "3.1.3"]
                             [lein-emr "0.2.0-SNAPSHOT"]]}})
