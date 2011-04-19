(ns forma.source.modis)

;; From the [user's guide](http://goo.gl/uoi8p) to MODIS product MCD45
;; (burned area): "The MODIS data are re-projected using an equiareal
;; sinusoidal projection, defined on a sphere of radius 6371007.181 m,
;; and with the Greenwich meridian as the central meridian of the
;; projection." The full MODIS grid has 18 vertical and 36 horizontal
;; tiles. Each tile is subdivided into pixels, based on the dataset's
;; spatial resolution.

(def rho 6371007.181)

(def h-tiles 36)
(def v-tiles 18)

(def pixels-at-res
  {"250" 4800
   "500" 2400
   "1000" 1200})

(def temporal-res
  {"MCD45A1" "32"
   "MOD13Q1" "16"
   "MOD13A1" "16"
   "MOD13A2" "16"
   "MOD13A3" "32"})

(def
  #^{:doc "Set of coordinate pairs for all MODIS tiles that contain
actual data. This set is calculated by taking a vector of offsets,
representing the first horizontal tile containing data for each row of
tiles. (For example, the data for row 1 begins with tile 14,
horizontal.)  For a visual representation of the MODIS grid and its
available data, see http://remotesensing.unh.edu/modis/modis.shtml"}
  valid-tiles
  (let [offsets [14 11 9 6 4 2 1 0 0 0 0 1 2 4 6 9 11 14]]
    (set (for [v-tile (range v-tiles)
               h-tile (let [shift (offsets v-tile)]
                        (range shift (- h-tiles shift)))]
           [h-tile v-tile]))))

(defn valid-modis?
  "Checks that the supplied values correspond to a valid MODIS tile,
  at the specified resolution."
  [res mod-h mod-v sample line]
  (let [edge (pixels-at-res res)]
    (and (contains? valid-tiles [mod-h mod-v])
         (< sample edge)
         (< line edge))))

(defn tilestring->xy
  "Extracts integer representations of the MODIS X and Y coordinates
referenced by the supplied MODIS tilestring, of format 'XXXYYY'."
  [tilestr]
  (map (comp #(Integer. %)
             (partial apply str))
       (partition 3 tilestr)))
