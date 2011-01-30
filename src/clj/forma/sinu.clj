;; I want to go ahead and rewrite the code found at this site:
;; http://landweb.nascom.nasa.gov/developers/tilemap/note.html
;; Now, as referenced here:
;; http://www.dfanning.com/map_tips/modis_overlay.html
;; NASA stopped using integerized sinusoidal projection in collection 3, and moved on to strictly
;; sinusoidal in collection 4. I believe that NDVI uses a straight up sinusoidal, so that's all we need to worry about.
;; 38,800 chunks for the 1km data means that to do a straight lookup for everything on the rain table is going to take,
;; roughly, 90 minutes (distributed across machines). Gotta be a faster way to do this!


(ns forma.sinu)

(def *rho* 6371007.181)
(def *pi* (Math/PI))
(def *x-tiles* 36)
(def *y-tiles* 18)

(def pixels-at-res
  {:250 4800
   :500 2400
   :1000 1200})

(defn cos
  "Takes the cosine of the supplied angle. Angle must be in degrees."
  [angle]
  (Math/cos (Math/toRadians angle)))

(defn degs-by-rho
  "Multiplies the argument by rho and converts the answer to radians."
  [arg]
  (* arg *rho* (/ *pi* 180)))

(defn sinusoidal-x
  "Returns the sinusoidal projection's x coordinate in meters for a given lat and long (in degrees)."
  [lat lon]
  (degs-by-rho (* (cos lat) lon)))

(defn sinusoidal-y
  "Returns the sinusoidal projection's y coordinate in meters for a given lat and long (in degrees)."
  [lat lon]
  (degs-by-rho lat))

(defn lat-long
  "Computes the latitude and longitude for a given set of sinusoidal map coordinates (in meters)."
  [x y]
  (let [lat (/ y *rho*)
        lon (/ x (* *rho* (Math/cos lat)))]
    (map #(Math/toDegrees %) [lat lon])))

(def min-x (sinusoidal-x 0 -180))
(def min-y (sinusoidal-y -90 0))
(def max-y (sinusoidal-y 90 0))

(defn pixel-length
  "The length, in meters, of the edge of a pixel at a given resolution."
  [res]
  (let [pixel-span (/ (- max-y min-y) *y-tiles*)
        total-pixels (pixels-at-res res)]
    (/ pixel-span total-pixels)))

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