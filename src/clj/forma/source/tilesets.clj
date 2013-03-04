(ns forma.source.tilesets
  (:use [clojure.set :only (union)]))

(def country-tiles
  "MODIS tiles for the selected countries, identified by their ISO3
  codes where each two-vector is the horizontal and vertical position
  of the MODIS tile `[mod-h mod-v]`"
  {:AGO #{[19 9] [20 9]}
   :ARG #{[11 11] [12 11] [13 11] [13 12]}
   :AUS #{[31 10] [31 11] [32 10]}
   :BDI #{[20 9] [21 9]}
   :BEN #{[18 7] [18 8]}
   :BFA #{[17 8] [18 7]}
   :BGD #{[25 6] [26 6]}
   :BHS #{[10 6] [11 6]}
   :BLZ #{[9 7]}
   :BOL #{[11 9] [11 10] [11 11] [12 10] [12 11]}
   :BRA #{[10 8] [10 9] [11 8] [11 9] [11 10]
          [12 8] [12 9] [12 10] [12 11] [13 8]
          [13 9] [13 10] [13 11] [13 12] [14 9] [14 10] [14 11]}
   :BTN #{[25 6] [26 6]}
   :CAF #{[19 8] [20 8]}
   :CHL #{[11 10] [11 11]}
   :CHN #{[24 5] [25 6] [26 6] [27 5] [27 6] [28 6] [28 7]}
   :CIV #{[17 8]}
   :CMR #{[18 8] [19 8]}
   :COD #{[19 8] [19 9] [20 8] [20 9] [21 8] [21 9]}
   :COG #{[19 8] [19 9]}
   :COL #{[10 7] [10 8] [10 9] [11 7] [11 8] [11 9]}
   :COM #{[22 10]}
   :CRI #{[9 7] [9 8]}
   :CUB #{[10 6] [10 7] [11 6] [11 7]}
   :DOM #{[11 7]}
   :ECU #{[9 8] [9 9] [10 8] [10 9]}
   :ETH #{[21 8]}
   :GAB #{[18 8] [18 9] [19 8] [19 9]}
   :GHA #{[17 8] [18 7] [18 8]}
   :GIN #{[16 8] [17 8]}
   :GLP #{[12 7]}
   :GNQ #{[18 8] [19 8]}
   :GRD #{[11 7]}
   :GTM #{[9 7]}
   :GUF #{[12 8]}
   :GUY #{[11 8] [12 8]}
   :HND #{[9 7]}
   :HTI #{[10 7] [11 7]}
   :IDN #{[27 8] [27 9] [28 8] [28 9] [29 8] [29 9] [30 8] [30 9] [31 9] [32 9]}
   :IND #{[24 5] [24 6] [24 7] [25 6] [25 7] [25 8] [26 6] [26 7] [27 7] [27 8]}
   :JAM #{[10 7]}
   :JPN #{[29 5] [29 6]}
   :KEN #{[21 8] [21 9]}
   :KHM #{[27 7] [28 7]}
   :KNA #{[11 7] [12 7]}
   :LAO #{[27 6] [27 7] [28 7]}
   :LBR #{[16 8] [17 8]}
   :LKA #{[25 8] [26 8]}
   :MDG #{[22 10] [22 11]}
   :MEX #{[8 6] [8 7] [9 6] [9 7]}
   :MLI #{[18 7]}
   :MMR #{[26 6] [26 7] [27 6] [27 7] [27 8]}
   :MWI #{[21 9]}
   :MYS #{[27 8] [28 8] [29 8]}
   :NER #{[18 7]}
   :NGA #{[18 7] [18 8] [19 8]}
   :NIC #{[9 7]}
   :NPL #{[24 6] [25 6]}
   :PAK #{[24 5] [24 6]}
   :PAN #{[9 8] [10 8]}
   :PER #{[9 9] [10 9] [11 9] [11 10] [10 10]}
   :PHL #{[29 7] [29 8] [30 7] [30 8]}
   :PNG #{[31 9] [32 9] [32 10] [33 9] [33 10]}
   :PRI #{[11 7]}
   :PRK #{[27 5]}
   :PRY #{[12 10] [12 11] [13 11]}
   :REU #{[23 11]}
   :RWA #{[20 9] [21 9]}
   :SDN #{[20 8] [21 8]}
   :SLB #{[33 9] [33 10]}
   :SLE #{[16 8]}
   :SLV #{[9 7]}
   :STP #{[18 8]}
   :SUR #{[12 8]}
   :TCD #{[19 8] [20 8]}
   :TGO #{[18 7] [18 8]}
   :THA #{[27 6] [27 7] [27 8] [28 7] [28 8]}
   :TTO #{[11 7] [11 8] [12 7]}
   :TWN #{[28 6] [29 6]}
   :TZA #{[20 9] [21 9]}
   :UGA #{[20 8] [20 9] [21 8] [21 9]}
   :URY #{[13 11] [13 12]}
   :USA #{[8 6] [9 6] [10 6] [10 7]}
   :VEN #{[10 7] [10 8] [11 7] [11 8] [12 8]}
   :VIR #{[11 7]}
   :VNM #{[27 6] [27 7] [28 7] [28 8]}
   :ZMB #{[20 9] [21 9]}})

(defn tile-set
  "set of unique MODIS tiles for the specified
  countries (union). Example usage:

    (tile-set [8 6] :PHL)
    ;=> #{[30 7] [29 7] [30 8] [29 8] [8 6]}

    (count (tile-set :all))
    ;=> 77"
  [& inputs]
  (->> (if (contains? (set inputs) :all)
         (keys country-tiles)
         inputs)
       (map #(if (keyword? %)
               (country-tiles %)
               #{%}))
       (apply union)))

(defn remove-tiles-by-iso
  "Remove tiles from set of tiles for given `iso-codes`. Useful for
   avoiding re-processing tiles unnecessarily.

   Usage:
     (tile-set :PER)
     ;=> #{[10 9] [11 10] [9 9] [10 10] [11 9]}

     (let [ts (tile-set :PER)]
       (remove-tiles-by-iso ts :BRA))
     ;=> #{[9 9] [10 10]}"
  [t-s & iso-codes]
  (let [remove-set (apply tile-set iso-codes)]
    (apply disj t-s remove-set)))
