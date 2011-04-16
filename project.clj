(defproject forma "0.1.0-SNAPSHOT"
  :description "[FORMA](http://goo.gl/4YTBw) gone Functional."
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :warn-on-reflection true
  :marginalia {:javascript ["mathjax/MathJax.js"]}
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"
                 "sonatype" "http://oss.sonatype.org/content/repositories/releases"}
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [incanter "1.2.3"]
                 [cascalog "1.7.0-SNAPSHOT"]
                 [clj-time "0.3.0"]
                 [org.clojars.sritchie09/gdal-java-native "1.8.0"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]
                     [marginalia "0.5.0"]
                     [midje "1.1"]
                     [org.cloudhoist/pallet "0.4.13"
                      :exclusions [org.jclouds/jclouds-compute
                                   org.jclouds/jclouds-blobstore
                                   org.jclouds/jclouds-scriptbuilder
                                   org.jclouds/jclouds-aws
                                   org.jclouds/jclouds-bluelock
                                   org.jclouds/jclouds-gogrid
                                   org.jclouds/jclouds-rackspace
                                   org.jclouds/jclouds-rimuhosting
                                   org.jclouds/jclouds-slicehost
                                   org.jclouds/jclouds-terremark
                                   org.jclouds/jclouds-jsch
                                   org.jclouds/jclouds-log4j
                                   org.jclouds/jclouds-enterprise]]
                     [org.jclouds/jclouds-all "1.0-beta-9b"]
                     [org.jclouds.driver/jclouds-jsch "1.0-beta-9b"]
                     [org.jclouds.driver/jclouds-log4j "1.0-beta-9b"]
                     [org.jclouds.driver/jclouds-enterprise "1.0-beta-9b"]
                     [org.cloudhoist/automated-admin-user "0.4.0"]
                     [org.cloudhoist/java "0.4.0"]
                     [com.jcraft/jsch "0.1.42"]
                     [log4j/log4j "1.2.14"]
                     [vmfest/vmfest "0.2.2"]])
