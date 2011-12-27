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

(defn fire-datachunk
  [date mh mv sample line val]
  (io/mk-chunk "fire" "01" date
               (io/pixel-location "1000" mh mv sample line)
               val))

(fact?- "Projecting fires into 1000m resolution."
        [[(fire-datachunk "2011-03-15" 31 11 181 1087 (schema/fire-value 1 1 1 1))]
         [(fire-datachunk "2011-03-15" 33 9  213 505  (schema/fire-value 0 0 0 1))]
         [(fire-datachunk "2011-03-15" 31 11 282 997  (schema/fire-value 0 1 0 1))]
         [(fire-datachunk "2011-03-15" 31 11 324 775  (schema/fire-value 0 0 0 1))]
         [(fire-datachunk "2011-03-15" 30 11 492 931  (schema/fire-value 0 1 0 1))]]
        (->> (hfs-textline daily-fires-path)
             fire-source-daily
             (reproject-fires "1000"))

        [[(fire-datachunk "2000-11-01" 32 9 759 715 (schema/fire-value 0 1 0 1))]
         [(fire-datachunk "2000-11-01" 32 9 761 715 (schema/fire-value 1 1 1 1))]
         [(fire-datachunk "2000-11-01" 33 9 902 940 (schema/fire-value 0 0 0 1))]
         [(fire-datachunk "2000-11-01" 33 9 907 942 (schema/fire-value 1 1 1 1))]
         [(fire-datachunk "2000-11-01" 33 9 908 947 (schema/fire-value 0 0 0 1))]]
        (->> (hfs-textline monthly-fires-path)
             fire-source-monthly
             (reproject-fires "1000")))
