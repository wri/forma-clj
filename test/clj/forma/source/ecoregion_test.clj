(ns forma.source.ecoregion-test
  (:use [forma.source.ecoregion] :reload)
  (:use [midje sweet cascalog])
  (:require [forma.utils :as u]
            [forma.source.gadmiso :only (text-map)]))

(def FIRST-LINE
  "10101\t\"Admiralty Islands lowland rain forests\"\t\"Papua New Guinea\"\t\"Papua New Guinea\"\t21\t\"New Guinea and Islands\"\t4693\t6")
(fact "Check `ecoregion-coll`."
  (first ecoregion-coll) => FIRST-LINE)

(fact "Check `parse-line`."
  (line->eco-dict FIRST-LINE) => {10101 {:regionid 21, :region "New Guinea and Islands"
                                         :hits 6 :pixels 4693
                                         :hit-share 0.001278499893458342}})

(fact "Check `eco-dict`".
  (eco-dict 10101) => {:regionid 21, :region "New Guinea and Islands"
                       :hits 6 :pixels 4693 :hit-share 0.001278499893458342})

(fact "Check `pixels-by-super-region`."
  (pixels-by-super-region [[FIRST-LINE]])
  => (produces [[21 "\"New Guinea and Islands\"" 4693]]))

(fact "Check `hits-by-super-region`."
  (hits-by-super-region [[FIRST-LINE]])
  => (produces [[21 "\"New Guinea and Islands\"" 6]]))

(fact "Check `summarize-by-super-region`."
  (summarize-super-regions [[FIRST-LINE]])
  => (produces [[21 "New Guinea and Islands" 4693 6 0.0012784998934583422]]))

(fact "Check `get-super-ecoregion`."
  (get-super-ecoregion 40145) => 15
  (get-super-ecoregion 10101) => 21)

(fact "Check `get-hit-share`."
  (get-hit-share 40145) => (roughly 0.1073)
  (get-hit-share 10101) => (roughly 0.001278))

(fact "Check `get-ecoregion`."
  (get-ecoregion 10101 :super-ecoregions true) => 21
  (get-ecoregion 10111 :super-ecoregions true) => 10111
  (get-ecoregion 10101 :super-ecoregions false) => 10101
  (get-ecoregion 10111 :super-ecoregions false) => 10111)

(fact "Check `matches-super-ecoregion?`."
  (matches-super-ecoregion? 10101 25) => true
  (matches-super-ecoregion? 10101 24) => false)

(fact "Check `super-ecoregion->ecoregions`."
  (super-ecoregion->ecoregions 25) => [60108 60147 60117 60149 60124 60125 60158]
  (super-ecoregion->ecoregions 26) => [60128 60163 60101 60165 60105 60142 60174
                                       60178 60121 60153 60156])
