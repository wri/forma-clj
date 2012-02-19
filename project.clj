(defproject forma "0.2.0-SNAPSHOT"
  :description "[FORMA](http://goo.gl/4YTBw) gone Functional."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :dev-resources-path "dev"
  :repositories {"conjars" "http://conjars.org/repo/"}
  :marginalia {:javascript ["mathjax/MathJax.js"]}
  :javac-options {:debug "true" :fork "true"}
  :checksum-deps true
  :jvm-opts ["-XX:MaxPermSize=128M" "-Xms1024M" "-Xmx2048M" "-server"]
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.cli "0.1.0"]
                 [clojure-csv/clojure-csv "1.3.2"]
                 [org.clojure/math.numeric-tower "0.0.1"]
                 [incanter/incanter-core "1.3.0-SNAPSHOT"]
                 [clj-time "0.3.4"]
                 [forma/gdal "1.8.0"]
                 [forma/jblas "1.2.1"]
                 [cascalog "1.9.0-wip"]
                 [cascalog-checkpoint "0.1.1"]
                 [backtype/dfs-datastores "1.1.0"]
                 [backtype/dfs-datastores-cascading "1.1.1"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [incanter/incanter-charts "1.3.0-SNAPSHOT"]
                     [midje-cascalog "0.4.0"]]
  :aot [forma.hadoop.pail
        forma.hadoop.jobs.scatter
        forma.hadoop.jobs.preprocess
        forma.hadoop.jobs.modis
        forma.hadoop.jobs.timeseries])


