(ns forma.gfw.cdm
  "This namespace defines functions for transforming coordinate-based FORMA data
  alerts into Google map tile coordinates:

  https://developers.google.com/maps/documentation/javascript/maptypes#TileCoordinates

  Many of the functions here were ported from the following Python script:

  http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/globalmaptiles.py
  "
  (:use [cascalog.api])
  (require [clojure.math.numeric-tower :as math]
           [cascalog.ops :as c]))

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
          yt-flipped (-> (- (math/expt 2 zoom) 1)
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
       (/ initial-res (math/expt 2 zoom)))))

;; Map of lat/lon min and max values.
(def latlon-range {:lat-min -90 :lat-max 90 :lon-min -180 :lon-max 180})

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
  (let [res (/ initial-res (math/expt 2 zoom))
        yp (-> (+ ym origin-shift)
               (/ res))
        xp (-> (+ xm origin-shift)
               (/ res))]
    [xp yp]))

(defn meters->maptile
  [xm ym zoom]
  "Returns the map tile coordinates [x y zoom] for a given x, y (in meters),
   resolution (in meters) and zoom level."
  (let [res (/ initial-res (math/expt 2 zoom))
        [xp yp] (meters->pixels-direct xm ym zoom)
        yt (-> (/ yp map-tile-dim)
               (Math/ceil)
               (- 1)
               (int))
        xt (-> (/ xp map-tile-dim)
               (Math/ceil)
               (- 1)
               (int))
        yt-flipped (-> (- (math/expt 2 zoom) 1)
                       (- yt))]
    [xt yt-flipped zoom]))

(defbufferop agg-sort-by-date
  "Aggregate dates and counts by xyz coordinate, sorting the counts by
  date. See usage example in tests."
  [tuples]
  (let [sorted (sort-by first tuples)]
    [[[(vec (map first sorted)) (vec (map second sorted))]]]))

(defn split-vecs
  "Split vector of nested vectors into two vectors of vectors. For use with
   output of `agg-sort-by-date` defbufferop.

   See usage example in tests."
  [v]
  [(vec (flatten (first v))) (vec (flatten (second v)))])

(defn vec->arr-str
  "Format vector as string array format for PostgreSQL."
  [v]
  (-> (str v)
      (clojure.string/replace "[" "{")
      (clojure.string/replace "]" "}")
      (clojure.string/replace " " ",")))

(defn arr-str->vec
  "Convert PostgreSQL array string to vector. Reverses `array-ify`."
  [s]
  (-> s
      (clojure.string/replace "{" "[")
      (clojure.string/replace "}" "]")
      (clojure.string/replace "," " ")
      (read-string)))

(defn agg-xyz
  []
  (<- [?x ?y ?z ?period ?count :> ?x2int ?y2int ?z-new ?periods ?counts]
      (/ ?x 2 :> ?x2)
      (/ ?y 2 :> ?y2)
      (math/floor ?x2 :> ?x2int)
      (math/floor ?y2 :> ?y2int)
      (dec ?z :> ?z-new)
      (agg-sort-by-date ?period ?count :> ?periods-counts)
      (split-vecs ?periods-counts :> ?periods ?counts)))

(defn zoom-out
  "Calculate x or y value at next zoom level."
  [n]
  (math/floor (/ n 2)))

(defn gen-tiles
  "Generate all tile levels through `min-z` (e.g. 7) for a given
  initial `xyz` tuple."
  [x y z min-z]
  (if (= z min-z)
     [[x y z]]
     (conj (gen-tiles (zoom-out x) (zoom-out y) (dec z) min-z) [x y z])))

(defmapcatop gen-tiles-mapcat
  "Wrap `gen-tiles` in `mapcatop` so each zoom level is emitted on a new line."
  [x y z min-z]
  (gen-tiles x y z min-z))


(defn gen-all-zooms
  "Cascalog query generates zoom levels through `min-z` for a given
  source of `xyz` tuples. For lower zoom levels, sets of `xyz-period`
  tuples are counted up. For a given `xyz-period` tuple, that gives us
  the number of pixels at the next higher zoom level that fall within
  the given `xyz` tuple for a given period."
  [src min-z]
  (<- [?x2 ?y2 ?z2 ?period ?count]
      (src ?x ?y ?z ?period)
      (gen-tiles-mapcat ?x ?y ?z min-z :> ?x2 ?y2 ?z2)
      (c/count ?count)))

(defn agg-periods-counts
  "Cascalog query aggregates periods and counts in long form into vectors.

   For example:

     [[25032 36353 16 71 2]
      [25032 36353 16 72 1]]

     becomes

     [[25032 36353 16 [71 72] [2 1]]]."
  [src]
  (<- [?x ?y ?z ?periods-arr ?counts-arr]
      (src ?x ?y ?z ?period ?count)
      (agg-sort-by-date ?period ?count :> ?periods-counts)
      (split-vecs ?periods-counts :> ?periods ?counts)
      (vec->arr-str ?periods :> ?periods-arr)
      (vec->arr-str ?counts :> ?counts-arr)))
