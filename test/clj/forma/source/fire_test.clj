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

(facts
  "Test `valid-fire?`. Note that the gap in the fire observations is
   seen in the actual data."
  (let [expected-length 12]
    (valid-fire?
     (str "latitude,longitude,brightness,scan,track,acq_date"
          ",acq_time,satellite,confidence,version,bright_t31,frp")
     expected-length)
    => false
    (valid-fire? "-16.701,137.752,338.2,1.7,1.3,2012-11-04, 01:25,T,89,5.0       ,298.1,63" expected-length) => true
    (valid-fire? "0.585,100.415,331.3,1.2,1.1,2012-09-18, 06:40,A,84,5.0A,75,5.0       ,302.4,26.3" expected-length) => false))

(future-fact
 "Test `fire-source`. Source mimics formatting of an input file.

  Test should pass, currently fails despite output seeming to be
  identical to the test result.

  Note that the gap in the fire is seen in the actual data."
  (let [src
        [[(str "latitude,longitude,brightness,scan,track,acq_date"
               ",acq_time,satellite,confidence,version,bright_t31,frp")]
         ["-16.701,137.752,338.2,1.7,1.3,2012-11-04, 01:25,T,89,5.0       ,298.1,63"]
         ["-16.163,133.733,338.2,1,1,2012-11-04, 01:25,T,89,5.0       ,306.5,27.8"]
         ["-16.164,133.743,336.1,1,1,2012-11-04, 01:25,T,87,5.0       ,306.3,24.1"]]        
        fire-src (fire-source src)]
    (<- [?name ?date ?t-res ?lat ?lon ?temp ?conf ?both ?count]
        (fire-src ?name ?date ?t-res ?lat ?lon ?fv)
        (thrift/unpack ?fv :> ?temp ?conf ?both ?count)))
  => (produces [["fire" "2012-11-04" "01" -16.701 137.752 1 1 1 1]
                ["fire" "2012-11-04" "01" -16.164 133.743 1 1 1 1]
                ["fire" "2012-11-04" "01" -16.163 133.733 1 1 1 1]]))
