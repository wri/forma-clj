(defproject forma "0.2.0-SNAPSHOT"
  :description "[FORMA](http://goo.gl/4YTBw) gone Functional."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :resources-path "resources"
  :dev-resources-path "dev"
  :repositories {"conjars" "http://conjars.org/repo/"}
  :marginalia {:javascript ["mathjax/MathJax.js"]}
  :javac-options {:debug "true" :fork "true"}
  ;; http://stackoverflow.com/questions/5839359/java-lang-outofmemoryerror-gc-overhead-limit-exceeded
  :jvm-opts ["-XX:MaxPermSize=128M" "-XX:+UseConcMarkSweepGC" "-Xms1024M" "-Xmx1048M" "-server"]
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.cli "0.1.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [clojure-csv/clojure-csv "1.3.2"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [incanter/incanter-core "1.3.0-SNAPSHOT"]
                 [clj-time "0.3.4"]
                 [forma/gdal "1.8.0"]
                 [forma/jblas "1.2.1"]
                 [cascalog "1.9.0-wip"]
                 [cascalog-lzo "0.1.0-wip7"]
                 [cascalog-checkpoint "0.1.1"]
                 [backtype/dfs-datastores "1.1.0"]
                 ;; [org.apache.thrift/libthrift "0.8.0"]
                 [backtype/dfs-datastores-cascading "1.1.1"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje-cascalog "0.4.0"]]
  :aot [forma.hadoop.pail
        forma.schema
        #"forma.hadoop.jobs.*"])
