;; I want to go ahead and rewrite the code found at this site:
;; http://landweb.nascom.nasa.gov/developers/tilemap/note.html Now, as
;; referenced here:
;; http://www.dfanning.com/map_tips/modis_overlay.html this os what
;; NASA stopped using integerized sinusoidal projection in
;; collection 3, and moved on to strictly sinusoidal in collection
;; 4. I believe that NDVI uses a straight up sinusoidal, so that's all
;; we need to worry about.  38,800 chunks for the 1km data means that
;; to do a straight lookup for everything on the rain table is going
;; to take, roughly, 90 minutes (distributed across machines). Gotta
;; be a faster way to do this!

(ns forma.sinu
  (:use [clojure.contrib.generic.math-functions :only (cos)]))


;; ## Constants

(def *rho* 6371007.181)
(def *x-tiles* 36)
(def *y-tiles* 18)

(def pixels-at-res
  {:250 4800
   :500 2400
   :1000 1200})

;; ## Helper Functions

(defn to-rad [angle] (Math/toRadians angle))
(defn to-deg [angle] (Math/toDegrees angle))

(defn distance
  "Calculates distance of magnitude from the starting point in a given direction."
  [dir start magnitude]
  (dir start magnitude))

(defn scale
  "Scales each element in a collection of numbers by the supplied factor."
  [fact sequence]
  (for [x sequence] (* x fact)))

(defn x-coord
  "Returns the x coordinate for a given (lat, long) point."
  [point]
  (first point))

(defn y-coord
  "Returns the y coordinate for a given (lat, long) point."  
  [point]
  (second point))

(defn sinu-xy
  "Computes the sinusoidal x and y coordinates for the supplied latitude and longitude (in radians)."
    [lat lon]
  (scale *rho* [(* (cos lat) lon) lat]))

(defn sinu-deg-xy
  "Computes the sinusoidal x and y coordinates for the supplied latitude and longitude (in degrees)."  
  [lat lon]
  (apply sinu-xy (map to-rad [lat lon])))

(defn lat-long
  "Computes the latitude and longitude for a given set of sinusoidal map coordinates (in meters)."
  [x y]
  (let [lat (/ y *rho*)
        lon (/ x (* *rho* (cos lat)))]
    (map to-deg [lat lon])))


;; These are the meter values of the minimum possible x and y values
;; on a sinusoidal projection. Things get weird at the corners, so I
;; compute the minimum x and y values separately, along the
;; projection's axes.

(def min-x (x-coord (sinu-deg-xy 0 -180)))
(def min-y (y-coord (sinu-deg-xy -90 0)))
(def max-y (y-coord (sinu-deg-xy 90 0)))

(defn pixel-length
  "The length, in meters, of the edge of a pixel at a given resolution."
  [res]
  (let [pixel-span (/ (- max-y min-y) *y-tiles*)
        total-pixels (pixels-at-res res)]
    (/ pixel-span total-pixels)))

;; ## Main Conversion Functions

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