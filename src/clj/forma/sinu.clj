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
  "Returns the sinusoidal projection's x coordinate in meters for a given lat and long (in degrees)."
  [lat lon]
  (degs-by-rho (* (Math/cos (Math/toRadians lat)) lon)))

(defn y-from-geo
  "Returns the sinusoidal projection's y coordinate in meters for a given lat and long (in degrees)."
  [lat lon]
  (degs-by-rho lat))

(def min-x (x-from-geo 0 -180))
(def min-y (y-from-geo -90 0))
(def max-y (y-from-geo 90 0))

(defn pixel-length
  "The length, in meters, of the edge of a pixel at a given resolution."
  [res]
  (let [y-tiles 18
        pixel-span (/ (- max-y min-y) y-tiles)]
    (/ pixel-span (pixels-at-res res))))

(defn lat-long
  "Computes the latitude and longitude for a given set of sinusoidal map coordinates (in meters)."
  [x y]
  (let [lat (/ y *rho*)
        lon (/ x (* *rho* (Math/cos lat)))]
    (map #(Math/toDegrees %) [lat lon])))

(defn distance
  "Calculates distance of magnitude from the starting point in a given direction."
  [dir start magnitude]
  (dir start magnitude))

(defn scale
  "Scales each element in a collection of numbers by the supplied factor."
  [fact seq]
  (for [x seq] (* x fact)))

(defn pixel-coords
  "returns the row and dimension of the pixel on the global MODIS grid."
  [mod-y mod-x line sample res]
  (let [edge-pixels (pixels-at-res res)]
    (map + [sample line] (scale edge-pixels [mod-x mod-y]))))

(defn map-coords
  "Returns the map position in meters for a given MODIS tile coordinate at the specified resolution."
  [mod-y mod-x line sample res]
  (let [edge-length (pixel-length res)
        half-edge (/ edge-length 2)
        pix-pos (pixel-coords mod-y mod-x line sample res)
        magnitudes (map #(+ half-edge %) (scale edge-length pix-pos))]
    (map distance [+ -] [min-x max-y] magnitudes)))

(defn geo-coords
  "Returns the latitude and longitude for a given group of MODIS tile coordinates at the specified resolution."
  [mod-y mod-x line sample res]
  (apply lat-long (map-coords mod-y mod-x line sample res)))