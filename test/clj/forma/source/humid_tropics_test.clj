(ns forma.source.humidtropics-test
  (:use [forma.source.humidtropics] :reload)
  (:use [midje.sweet]))

(fact
  "Check that `eco-id-seq` is reading `humid-tropics-ids.csv` correctly"
  (count eco-id-seq) => 231
  (first eco-id-seq) => "10101")

(fact
  "Check that `humid-tropics-ids` is parsing ecoids into a set"
  (set? humid-tropics-ids))

(fact
  "Check `in-humid-tropics?`"
  (in-humid-tropics? -1) => false
  (in-humid-tropics? 10101) => true)








