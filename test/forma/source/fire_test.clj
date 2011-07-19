(ns forma.source.fire-test
  (:use [forma.source.fire] :reload)
  (:use cascalog.api
        [midje sweet cascalog])
  (:require [forma.hadoop.io :as io]
            [forma.testing :as t]))

(def daily-fires-path
  (t/dev-path "/testdata/FireDaily/MCD14DL.2011074.txt"))

(def monthly-fires-path
  (t/dev-path "/testdata/FireMonthly/MCD14ML.200011.005.01.asc"))

(fact?- "Projecting fires into 1000m resolution."
        [["fire" "1000" "01" "031011" "2011-03-15" 181 1087 (io/fire-tuple 1 1 1 1)]
         ["fire" "1000" "01" "033009" "2011-03-15" 213 505  (io/fire-tuple 0 0 0 1)]
         ["fire" "1000" "01" "031011" "2011-03-15" 282 997  (io/fire-tuple 0 1 0 1)]
         ["fire" "1000" "01" "031011" "2011-03-15" 324 775  (io/fire-tuple 0 0 0 1)]
         ["fire" "1000" "01" "030011" "2011-03-15" 492 931  (io/fire-tuple 0 1 0 1)]]
        (->> (hfs-textline daily-fires-path)
             fire-source-daily
             (reproject-fires "1000"))

        [["fire" "1000" "01" "032009" "2000-11-01" 759 715 (io/fire-tuple 0 1 0 1)]
         ["fire" "1000" "01" "032009" "2000-11-01" 761 715 (io/fire-tuple 1 1 1 1)]
         ["fire" "1000" "01" "033009" "2000-11-01" 902 940 (io/fire-tuple 0 0 0 1)]
         ["fire" "1000" "01" "033009" "2000-11-01" 907 942 (io/fire-tuple 1 1 1 1)]
         ["fire" "1000" "01" "033009" "2000-11-01" 908 947 (io/fire-tuple 0 0 0 1)]]
        (->> (hfs-textline monthly-fires-path)
             fire-source-monthly
             (reproject-fires "1000")))
