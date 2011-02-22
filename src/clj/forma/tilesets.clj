(ns forma.tilesets
  (:use forma.modis))

(def
  #^{:doc "Set of coordinate pairs for all MODIS tiles that contain
actual data. This set is calculated by taking a vector of offsets,
representing the first horizontal tile containing data for each row of
tiles. (For example, the data for row 1 begins with tile 14,
horizontal.)  For a visual representation of the MODIS grid and its
available data, see http://remotesensing.unh.edu/modis/modis.shtml"}
  good-tiles
  (let [offsets [14 11 9 6 4 2 1 0 0 0 0 1 2 4 6 9 11 14]]
    (vec (for [v-tile (range v-tiles)
               h-tile (let [shift (offsets v-tile)]
                        (range shift (- h-tiles shift)))]
           [h-tile v-tile]))))