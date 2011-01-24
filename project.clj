(defproject forma "1.0.0-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [cascalog "1.6.0-SNAPSHOT"]
                 ]
  :native-dependencies [[org.clojars.sritchie09/gdal-java-osx-native-deps "1.0.0"]]
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]
                     [lein-eclipse "1.0.0"]
                     [marginalia "0.2.3"]
                     [native-deps "1.0.5"]
                     ])

