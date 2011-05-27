(ns forma.source.tilesets-test
  (:use forma.source.tilesets
        clojure.test
        midje.sweet))

(facts
  (tile-set :MYS) => #{[27 8] [28 8] [29 8]}
  (tile-set :MYS [27 8]) => #{[27 8] [28 8] [29 8]}
  (tile-set :MYS [8 7]) => #{[27 8] [28 8] [29 8] [8 7]})


