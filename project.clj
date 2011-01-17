(defproject forma-hadoop "1.0.0-SNAPSHOT"
  :source-path "src/clj"
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [org.n52.wps.gdal/gdal "1.0.0"]
                 ;; [org.hdfgroup/hdf-java "2.6.1"]
                 [cascalog "1.6.0-SNAPSHOT"]
                 ]
    :repositories {"52north" "http://52north.org/maven/repo/releases/"}
    :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]
                     ])

