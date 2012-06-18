(defproject forma/forma "0.2.0-SNAPSHOT"
  :description "[FORMA](http://goo.gl/4YTBw) gone Functional."
  :repositories {"conjars" "http://conjars.org/repo/"}
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :marginalia {:javascript ["mathjax/MathJax.js"]}
  :resources-path "resources"
  :dev-resources-path "dev"
  :jvm-opts ["-XX:MaxPermSize=128M"
             "-XX:+UseConcMarkSweepGC"
             "-Xms1024M" "-Xmx1048M" "-server"]

  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.cli "0.1.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [clojure-csv/clojure-csv "1.3.2"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [incanter/incanter-core "1.3.0-SNAPSHOT"]
                 [clj-time "0.3.4"]
                 [forma/gdal "1.8.0"]
                 [forma/jblas "1.2.1"]
                 [cascalog "1.9.0-wip12"]
                 [cascalog-lzo "0.1.0-wip12"]
                 [cascalog-checkpoint "0.1.1"]
                 [backtype/dfs-datastores "1.1.3-SNAPSHOT"]
                 [backtype/dfs-datastores-cascading "1.1.4"]
                 [org.apache.thrift/libthrift "0.8.0"
                  :exclusions [org.slf4j/slf4j-api]]
                 [com.google.protobuf/protobuf-java "2.4.0a"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje-cascalog "0.4.0"]]
  :aot [forma.hadoop.pail, forma.schema, #"forma.hadoop.jobs.*"])
