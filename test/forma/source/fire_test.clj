(ns forma.source.fire-test
  (:use [forma.source.fire] :reload)
  (:use cascalog.api
        [midje sweet cascalog])
  (:require [forma.testing :as t]
            [forma.thrift :as thrift]))

(def daily-fires-path
  (t/dev-path "/testdata/FireDaily/MCD14DL.2011074.txt"))

(def monthly-fires-path
  (t/dev-path "/testdata/FireMonthly/MCD14ML.200011.005.01.asc"))

(defn fire-datachunk
  [date mh mv sample line val]
  (thrift/DataChunk* "fire" (thrift/ModisPixelLocation* "1000" mh mv sample line) val "01" :date date))

(fact?- "Projecting fires into 1000m resolution."
        [[(fire-datachunk "2011-03-15" 31 11 181 1087 (thrift/FireValue* 1 1 1 1))]
         [(fire-datachunk "2011-03-15" 33 9  213 505  (thrift/FireValue* 0 0 0 1))]
         [(fire-datachunk "2011-03-15" 31 11 282 997  (thrift/FireValue* 0 1 0 1))]
         [(fire-datachunk "2011-03-15" 31 11 324 775  (thrift/FireValue* 0 0 0 1))]
         [(fire-datachunk "2011-03-15" 30 11 492 931  (thrift/FireValue* 0 1 0 1))]]
        (->> (hfs-textline daily-fires-path)
             fire-source-daily
             (reproject-fires "1000"))

        [[(fire-datachunk "2000-11-01" 32 9 759 715 (thrift/FireValue* 0 1 0 1))]
         [(fire-datachunk "2000-11-01" 32 9 761 715 (thrift/FireValue* 1 1 1 1))]
         [(fire-datachunk "2000-11-01" 33 9 902 940 (thrift/FireValue* 0 0 0 1))]
         [(fire-datachunk "2000-11-01" 33 9 907 942 (thrift/FireValue* 1 1 1 1))]
         [(fire-datachunk "2000-11-01" 33 9 908 947 (thrift/FireValue* 0 0 0 1))]]
        (->> (hfs-textline monthly-fires-path)
             fire-source-monthly
             (reproject-fires "1000")))
