(ns forma.gfw.cdm-test
  "This namespace defines Midge tests for the forma.gfw.cdm namespace."
  (:use forma.gfw.cdm
        forma.hadoop.jobs.cdm
        [midje sweet]))

(fact
  "A coordinate at (41.850033, -87.65005229999997) at zoom level 3 has map tile
  coordinates x = 2, y = 2." 
  (let [[x y] (latlon->tile 41.850033 -87.65005229999997 3)]    
    x => 2
    y => 2))



