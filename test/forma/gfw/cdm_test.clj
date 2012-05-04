(ns forma.gfw.cdm-test
  (:use forma.gfw.cdm
        [midje sweet]
        ))

(fact
  (lat->y 10) => (roughly 1118889.9748579583))

(fact
  (lon->x 10) => (roughly 1113194.9079327357))

(fact
  (res-at-zoom 2) => (roughly 39135.75848201024))

(fact
  (let [z 2
        res (res-at-zoom z)]
    (px-coord 10 res)) => (roughly 512.0002555207919))

(fact
  (px->tile 1000) => 3)

(fact
  (meters->tile 10 10 2) => [2 2])

(fact
  (tile->google-tile 10 10 2) => [10 -7])

(fact
  (latlon->google-tile 10 10 1) => [1 0])