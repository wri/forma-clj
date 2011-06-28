(defproject forma "0.2.0-SNAPSHOT"
  :description "[FORMA](http://goo.gl/4YTBw) gone Functional."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :dev-resources-path "dev"
  :marginalia {:javascript ["mathjax/MathJax.js"]}
  :javac-options {:debug "true" :fork "true"}
  :jvm-opts ["-XX:MaxPermSize=128M"]
  :repositories {"conjars" "http://conjars.org/repo/"
                 "sonatype"
                 "http://oss.sonatype.org/content/repositories/releases/"}
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [incanter "1.2.3" :exclusions [swank-clojure]]
                 [cascalog "1.7.0"]
                 [clj-time "0.3.0"]
                 [redd/thrift "0.5.0"]
                 [org.clojars.sritchie09/gdal-java-native "1.8.0"]
                 [backtype/cascading-thrift "0.1.0"
                  :exclusions [backtype/thriftjava]]
                 [redd/dfs-datastores "1.0.4"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [pallet-hadoop "0.3.0"]
                     [org.jclouds/jclouds-all "1.0.0"]
                     [org.jclouds.driver/jclouds-jsch "1.0.0"]
                     [org.jclouds.driver/jclouds-log4j "1.0.0"]
                     [log4j/log4j "1.2.14"]
                     [vmfest/vmfest "0.2.2"]
                     [swank-clojure "1.4.0-SNAPSHOT"]
                     [clojure-source "1.2.0"]
                     [marginalia "0.6.0"]
                     [midje "1.2-alpha3"]]
  :tasks [marginalia.tasks]
  :aot [forma.hadoop.jobs.preprocess
        forma.hadoop.jobs.load-tseries
        forma.hadoop.jobs.run-forma
        forma.source.fire])
