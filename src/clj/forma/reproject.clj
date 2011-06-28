;; The functions in this namespace allow reprojection of a gridded
;; (lat, lon) dataset at arbitrary resolution into the MODIS
;; sinusoidal grid at arbitrary resolution.

(ns forma.reproject
  (:require [forma.source.modis :as m]))

;; ### WGS84 -> MODIS Index Mapping
;;
;; If we have a dataset in WGS84 gridded at some spatial resolution,
;; to reproject into MODIS we'll need to generate lists that tell us
;; which WGS84 indices to sample for the data at each MODIS
;; pixel. (The following assume that WGS84 data is held in a row
;; vector.)

(defn bucket
  "Takes a floating-point value and step size, and returns the
  step-sized bucket into which the value falls. For example:

     (bucket 0.4 1.3)
     ;=> 3

     (bucket 0.9 1.3)
     ;=> 1"
  [step-size val]
  (->> step-size (/ 1) (* val) Math/floor int))

(defn travel
  "Returns the value obtained by traveling `idx` steps, each
  `step-size` large, in the supplied direction from the value
  `start`. (The function takes an extra half-step, so as to return the
  centroid of the point reached.) For example:

    (travel 0.5 + 0 2)
    => 1.25
    (start, travel, end at centroid.)
    0.0 --> 0.5 --> 1.0 -*- 1.5"
  [step-size dir start idx]
  (dir start (* (+ idx 0.5) step-size)))

(defn constrain
  "Constrains the supplied `val` to the range obtained by traveling
  `range` in the positive direction from the supplied starting
  value. If the value falls outside these bounds, it wraps around to
  the other edge."
  [range start val]
  (+ start (mod (- val start) range)))

(def constrain-lat (partial constrain 180 -90))
(def constrain-lon (partial constrain 360 -180))

(defn dimensions-for-step
  "returns the <horz, vert> dimensions of a WGS84 grid with the
  supplied spatial step-size between pixels."
  [step-size]
  {:pre [(pos? step-size)]}
  (map #(quot % step-size) [360 180]))

(defn line-torus
  "Returns the magnitude of the difference between the supplied value
  and the corner point within the supplied range. Values are assumed
  to wrap around at the edge of the range. For example,

    (line-torus + 10 -2 1) => 3
    (line-torus + 10 -2 -3) => 9

  The -3 value is assumed to have wrapped around the edge of the
  range. Note that this function will wrap any number of times:

    (line-torus + 10 -2 54) => 6"
  [dir range corner val]
  (constrain range 0 (dir (- val corner))))

(defn latlon->rowcol
  "Takes a coordinate pair and returns its [row, col] position on a
  WGS84 grid with the supplied spatial resolution and width in
  columns."
  [step-size lat-dir lon-dir lat-corner lon-corner lat lon]
  (map (partial bucket step-size)
       [(line-torus lat-dir 180 lat-corner lat)
        (line-torus lon-dir 360 lon-corner lon)]))

(defn rowcol->latlon
  "Returns the coordinates of the centroid of the point defined by
  `row` and `col` on an ascii grid with the supplied corner point and
  step size."
  [step-size lat-dir lon-dir lat-corner lon-corner row col]
  (let [move (partial travel step-size)]
    [(constrain-lat (move lat-dir lat-corner row))
     (constrain-lon (move lon-dir lon-corner col))]))

(defn valid-rowcol?
  "Determines whether or not the supplied ASCII row and column falls
  within the bounds of an ASCII grid with the supplied step size."
  [step row col]
  (let [[cols rows] (dimensions-for-step step)]
    (and (>= row 0) (< row rows)
         (>= col 0) (< col cols))))

(defn modis-indexer
  "Accepts WGS84 coordinates and returns the corresponding `[mod-h,
  mod-v, sample, line]` in the modis grid at the supplied resolution
  for an ASCII grid with the supplied step-size, corner coordinates
  and directions traveled for each axis."
  [m-res {:keys [step corner travel]} row col]
  {:pre [(valid-rowcol? step row col)]}
  (let [[xul yul] corner
        [x-dir y-dir] travel]
    (->> (rowcol->latlon step y-dir x-dir yul xul row col)
         (apply m/latlon->modis m-res))))

(defn wgs84-indexer
  "Accepts MODIS tile coordinates and returns the corresponding `[row,
  col]` within a WGS84 grid of values with the supplied step-size,
  corner coordinates and directions traveled for each axis."
  [m-res ascii-map mod-h mod-v sample line]
  {:pre [(m/valid-modis? m-res mod-h mod-v sample line)]}
  (let [{:keys [step corner travel]} ascii-map
        [lon-corner lat-corner] corner
        [lon-dir lat-dir] travel]
    (->> (m/modis->latlon m-res mod-h mod-v sample line)
         (apply latlon->rowcol step
                lat-dir lon-dir
                lat-corner lon-corner))))
