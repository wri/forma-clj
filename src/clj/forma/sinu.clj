;; I want to go ahead and rewrite the code found at this site:
;; http://landweb.nascom.nasa.gov/developers/tilemap/note.html
;; Now, as referenced here:
;; http://www.dfanning.com/map_tips/modis_overlay.html
;; NASA stopped using integerized sinusoidal projection in collection 3, and moved on to strictly
;; sinusoidal in collection 4. I believe that NDVI uses a straight up sinusoidal, so that's all we need to worry about.

(ns forma.sinu)

(def *rho* 6371007.181)

(def pixels-at-res
  {:250 4800
   :500 2400
   :1000 1200})

(defn degs-by-rho
  "Multiplies the argument by rho and converts the answer to radians."
  [arg]
  (* arg *rho* (/ (Math/PI) 180)))

(defn x-from-geo
  "Returns the sinusoidal projection's x coordinate in meters for a given latitude and longitude."
  [lat lon]
  (degs-by-rho (* (Math/cos (Math/toRadians lat)) lon)))

(defn y-from-geo
  "Returns the sinusoidal projection's y coordinate in meters for a given latitude and longitude."
  [lat lon]
  (degs-by-rho lat))

(def min-x (x-from-geo 0 -180))
(def min-y (y-from-geo -90 0))
(def max-y (y-from-geo 90 0))

(def grid-size
  (let [y-tiles 18]
    (/ (- max-y min-y) y-tiles)))

(defn lat-long
  "Computes the latitude and longitude for a given set of sinusoidal map coordinates (in meters)."
  [x y]
  (let [lat (/ y *rho*)
        lon (/ x (* *rho* (Math/cos lat)))]
    (map #(Math/toDegrees %) (vector lat lon))))


(defn image-coords
  "returns the row and dimension of the pixel on the global MODIS grid."
  [mod-y mod-x line sample res]
  (let [tile-size (pixels-at-res res)]
    (vector (+ sample
               (* mod-x tile-size))
            (+ line
               (* mod-y tile-size)))))

(defn map-coords
  "Returns the map position in meters for a given MODIS tile coordinate at the specified resolution."
  [mod-y mod-x line sample res]
  (let [tile-size (/ grid-size (pixels-at-res res))
        half-tile (/ tile-size 2)
        ul-x (+ min-x half-tile)
        ul-y (- max-y half-tile)
        pos (image-coords mod-y mod-x line sample res)]
    (vector (+ ul-x (* (first pos) tile-size))
            (- ul-y (* (second pos) tile-size)))))

(defn geo-coords
  "Returns the latitude and longitude for a given group of MODIS tile coordinates at the specified resolution."
  [mod-y mod-x line sample res]
  (let [map-point (map-coords mod-y mod-x line sample res)]
    (lat-long (first map-point) (second map-point))))