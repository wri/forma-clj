(ns forma.source.tilesets-test
  (:use forma.source.tilesets
        midje.sweet))

(tabular
 (fact "tile-set testing."
   (apply tile-set ?inputs) => ?result)
 ?inputs       ?result
 [:MYS]        #{[27 8] [28 8] [29 8]}
 [:MYS [27 8]] #{[27 8] [28 8] [29 8]}
 [:MYS [8 7]]  #{[27 8] [28 8] [29 8] [8 7]})

(fact
  "Test :all keyword with `tile-set`"
  (count (tile-set :all)) => 77)

(fact
  "Test `remove-tiles-by-iso`"
  (remove-tiles-by-iso (tile-set :PER) :BRA) => #{[9 9] [10 10]})
