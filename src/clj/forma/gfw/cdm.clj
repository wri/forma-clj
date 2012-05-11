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

;; Data structure for an x/y coordinate.
(defrecord Coordinate [y x])

;; Data structure for a map tile which has a coordinate and a map zoom level.
(defrecord MapTile [coordinate zoom])

;; Data structure for a FORMA alert.
(defrecord Alert [lat lon zoom]

  GlobalMercator

  (get-maptile
    [this]
    "Returns this alert represented as a MapTile."
    (let [{:keys [x y]} (pixels->tile this)
          zoom (:zoom this)
          ;; Moves coordinate origin from bottom-left to top-left corner of tile
          ty-top-left (-> (- (expt 2 zoom) 1)
                          (- y))]
      (MapTile. (Coordinate. ty-top-left x) zoom)))
  
  (latlon->meters
    [this]
    "Returns this alert coordinates as meters."
    (let [{:keys [lat lon zoom]} this
          mx (/ (* lon origin-shift) 180)
          my (-> (+ 90 lat)
                 (* Math/PI)
                 (/ 360)
                 (Math/tan)
                 (Math/log)
                 (/ (/ Math/PI 180))
                 (* (/ origin-shift 180)))]
      (Coordinate. my mx)))

  (meters->pixels
    [this]
     "Returns this alert coordinates as pixels."
    (let [{:keys [x y]} (latlon->meters this)
          res (resolution this)
          px (-> (+ x origin-shift)
                 (/ res))
          py (-> (+ y origin-shift)
                 (/ res))]
      (Coordinate. py px)))

  (pixels->tile
    [this]
    "Returns this alert coordinates as map tile coordinates."
    (let [{:keys [x y]} (meters->pixels this)
          res (resolution this)
          tx (-> (/ x map-tile-dim)
                 (Math/ceil)
                 (- 1)
                 (int))
          ty (-> (/ y map-tile-dim)
                 (Math/ceil)
                 (- 1)
                 (int))]
      (Coordinate. ty tx)))   

  (resolution
    [this]
    "Returns this alert map resolution in meters per pixel."
    (let [zoom (:zoom this)]
      (/ initial-res (expt 2 zoom)))))

(defn latlon->tile
  [lat lon zoom]
  "Returns tile [x y zoom] corresponding to a lat/lon at the given zoom."
  ;; TODO add precondition for lat and lon as Double values.
  (let [alert (Alert. lat lon zoom)
        tile (get-maptile alert)
        {:keys [coordinate zoom]} tile
        {:keys [x y]} coordinate]   
    [x y zoom]))
