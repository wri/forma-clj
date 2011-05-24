(ns forma.source.tilesets
  (:use forma.source.modis
        [clojure.set :only (union)]))

(def
  ^{:doc "MODIS tiles for the selected countries, identified by their
  ISO3 codes where each two-vector is the horizontal and vertical
  position of the MODIS tile [`mod-h` `mod-v`]"}
  country-tiles
  {:BGD #{[25 6] [26 6]}
   :LAO #{[27 6] [27 7] [28 7]}
   :IDN #{[27 8] [28 8] [29 8] [30 8] [27 9] [28 9] [29 9] [30 9] [31 9] [32 9]}
   :IND #{[24 5] [24 6] [25 6] [26 6] [24 7] [25 7] [26 7] [27 7] [25 8] [27 8]}
   :MMR #{[26 6] [27 6] [26 7] [27 7] [27 8]}
   :MYS #{[27 8] [28 8] [29 8]}
   :PHL #{[29 7] [30 7] [29 8] [30 8]}
   :THA #{[27 6] [27 7] [28 7] [27 8] [28 8]}
   :VNM #{[27 6] [27 7] [28 7] [28 8]}})

(defn tile-set
  "set of unique MODIS tiles for the specified
  countries (union). Example usage:

    (tile-set :IDN :MYS :PHL)"
  [& countries]
  (apply union
         (map country-tiles countries)))











