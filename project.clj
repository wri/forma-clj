(defproject forma "0.2.0-SNAPSHOT"
  :description "[FORMA](http://goo.gl/4YTBw) gone Functional."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :dev-resources-path "dev"
  :marginalia {:javascript ["mathjax/MathJax.js"]}
  :javac-options {:debug "true" :fork "true"}
  :run-aliases {:cluster forma.hadoop.cluster}
  :jvm-opts ["-XX:MaxPermSize=128M" "-Xms1024M" "-Xmx2048M" "-server"
             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :repositories {"releases" "http://oss.sonatype.org/content/repositories/releases/"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.cli "0.1.0"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [incanter/incanter-core "1.3.0-SNAPSHOT"]
                 [clj-time "0.3.3"]
                 [redd/thrift "0.5.0"]
                 [forma/gdal "1.8.0"]
                 [commons-lang "2.6"]   ;required for thrift
                 [cascalog "1.8.5-SNAPSHOT"]
                 [backtype/cascading-thrift "0.1.0"
                  :exclusions [backtype/thriftjava]]
                 [backtype/dfs-datastores "1.0.5"]
                 [backtype/dfs-datastores-cascading "1.0.5"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [incanter/incanter-charts "1.3.0-SNAPSHOT"]
                     [midje-cascalog "0.3.0"]]
  :aot [
        forma.hadoop.pail
        forma.hadoop.jobs.scatter
        forma.hadoop.jobs.preprocess
        forma.hadoop.jobs.modis
        forma.hadoop.jobs.timeseries
        ])
