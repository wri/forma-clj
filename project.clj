(defproject forma "1.0.0-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [incanter "1.2.3"]
                 [cascalog "1.7.0-SNAPSHOT"]
                 [clj-time "0.3.0-SNAPSHOT"]
                 [org.clojars.sritchie09/gdal-java "1.8.0"]
                 ]
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]
                     [lein-eclipse "1.0.0"]
                     [marginalia "0.2.3"]
                     [midje "1.0.1"]
                     ])