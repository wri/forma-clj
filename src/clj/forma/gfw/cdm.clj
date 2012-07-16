(ns forma.gfw.cdm
  "This namespace defines functions for transforming coordinate-based FORMA data
  alerts into Google map tile coordinates:

  https://developers.google.com/maps/documentation/javascript/maptypes#TileCoordinates

  Many of the functions here were ported from the following Python script:

  http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/globalmaptiles.py
  "
  (:use [clojure.math.numeric-tower :only (expt, round)]))

;; Exact radius of the Earth in meters for the WGS84 datum.
(def radius 6378137)

;; The dimension of a Google Map tile is 256x256 pixels.
(def map-tile-dim 256)

;; Circumference of the Earth in meters.
(def circumf (* (* 2 Math/PI) radius))

;; Initial pixel resolution in meters.
(def initial-res (/ circumf map-tile-dim))

;; The origin shift in meters.
(def origin-shift (/ circumf 2))

(defprotocol GlobalMercator
  "Protocol for generating map tile coordinates in Spherical Mercator
  projection EPSG:900913, from latitude and longitude coordinates in
  WGS84 Datum."
  (get-maptile [this])
  (latlon->meters [this])
  (resolution [this])
  (meters->pixels [this])
  (pixels->tile [this]))

;; Data structure for a FORMA alert.
(defrecord Alert [lat lon zoom]

  GlobalMercator

  (get-maptile
    [this]
    "Returns this alert represented as a MapTile."
    (let [[yt xt] (pixels->tile this)
          zoom (:zoom this)
          ;; Moves coordinate origin from bottom-left to top-left corner of tile
          yt-flipped (-> (- (expt 2 zoom) 1)
                          (- yt))]
      [xt yt-flipped zoom]))
    
  (latlon->meters
    [this]
    "Returns this alert coordinates as meters."
    (let [{:keys [lat lon zoom]} this
          xm (/ (* lon origin-shift) 180)
          ym (-> (+ 90 lat)
                 (* Math/PI)
                 (/ 360)
                 (Math/tan)
                 (Math/log)
                 (/ (/ Math/PI 180))
                 (* (/ origin-shift 180)))]
      [ym xm]))
  
  (meters->pixels
    [this]
    "Returns this alert coordinates as pixels."
    (let [[ym xm] (latlon->meters this)
          res (resolution this)
          yp (-> (+ ym origin-shift)
                 (/ res))
          xp (-> (+ xm origin-shift)
                 (/ res))]
      [yp xp]))

  (pixels->tile
    [this]
    "Returns this alert coordinates as map tile coordinates."
    (let [[yp xp] (meters->pixels this)
          res (resolution this)
          yt (-> (/ yp map-tile-dim)
                 (Math/ceil)
                 (- 1)
                 (int))
          xt (-> (/ xp map-tile-dim)
                 (Math/ceil)
                 (- 1)
                 (int))]
      [yt xt]))   
  
    (resolution
     [this]
     "Returns this alert map resolution in meters per pixel."
     (let [zoom (:zoom this)]
       (/ initial-res (expt 2 zoom)))))

;; Map of lat/lon min and max values.
(def latlon-range {:lat-min -90 :lat-max 90 :lon-min -180 :lon-max 180})

(defn read-latlon
  "Converts lat and lon values from string to number."
  [lat lon]
  {:pre [(= (type lat) java.lang.String), (= (type lon) java.lang.String)]}
  [ (read-string lat) (read-string lon)])

(defn latlon-valid?
  [lat lon]
  "Returns true if lat and lon are valid, otherwise returns false."
  (try
    (let [{:keys [lat-min lat-max lon-min lon-max]} latlon-range]
      (and (<= lat lat-max)
           (>= lat lat-min)
           (<= lon lon-max)
           (>= lon lon-min)))
    (catch Exception e false)))

(defn latlon->tile
  [lat lon zoom]
  {:pre [(latlon-valid? lat lon)]}
  "Returns the map tile coordinates [x y zoom] at a given lat, lon, and zoom."
  (get-maptile (Alert. lat lon zoom)))

(defn meters->pixels-direct
  [xm ym zoom]
  "Directly converts x and y coordinates to pixel coordinates at zoom level,
   without passing through latlon."
  (let [res (/ initial-res (expt 2 zoom))
        yp (-> (+ ym origin-shift)
               (/ res))
        xp (-> (+ xm origin-shift)
               (/ res))]
    [xp yp]))

(defn meters->tile
  [xm ym zoom]
  "Returns the map tile coordinates [x y zoom] for a given x, y (in meters),
   resolution (in meters) and zoom level."
  (let [res (/ initial-res (expt 2 zoom))
        [xp yp] (meters->pixels-direct xm ym zoom)
        yt (-> (/ yp map-tile-dim)
               (Math/ceil)
               (- 1)
               (int))
        xt (-> (/ xp map-tile-dim)
               (Math/ceil)
               (- 1)
                  (int))
        yt-flipped (-> (- (expt 2 zoom) 1)
                       (- yt))]
    [xt yt-flipped zoom]))