;; The functions in this namespace allow reprojection of a gridded
;; (lat, lon) dataset at arbitrary resolution into the MODIS
;; sinusoidal grid at arbitrary resolution.

(ns forma.reproject
  (:use cascalog.api
        [forma.matrix.utils :only (rowcol->idx)])
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
  [step val]
  (->> step (/ 1) (* val) Math/floor int))

(defn travel
  "travel along a grid with `step` in the direction given by `dir`
  from an initial position `start` to a position `pos` which is
  intended to be, for most applications, row or column within the
  grid.  Note that this takes you to the centroid of the row or column
  position that you specify."
  [step dir start pos]
  (dir start (* pos step) (/ step 2)))

(defn dimensions-for-step
  "returns the <horz, vert> dimensions of a WGS84 grid with the
  supplied spatial step between pixels."
  [step]
  (map #(quot % step) [360 180]))

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
  (mod (dir (- val corner)) range))

(defn latlon->rowcol
  "Takes a coordinate pair and returns its [row, col] position on a
  WGS84 grid with the supplied spatial resolution and width in
  columns."
  [step lat-dir lon-dir lat-corner lon-corner lat lon]
  (map (partial bucket step)
       [(line-torus lat-dir 180 lat-corner lat)
        (line-torus lon-dir 360 lon-corner lon)]))

(defn rowcol->latlon
  "Returns the coordinates of the centroid of the point defined by
  `row` and `col` on an ascii grid with the supplied corner point and
  step size."
  [step lat-dir lon-dir lat-corner lon-corner row col]
  (map (partial travel step)
       [lat-dir lon-dir]
       [lat-corner lon-corner]
       [row col]))

(defn modis-indexer
  "Accepts WGS84 coordinates and returns the corresponding `[mod-h,
  mod-v, sample, line]` in the modis grid at the supplied resolution
  for an ASCII grid with the supplied step-size, corner coordinates
  and directions traveled for each axis."
  [m-res ascii-map row col]
  (let [{:keys [step corner travel]} ascii-map
        [xul yul] corner
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
         (apply latlon->rowcol step lat-dir lon-dir lat-corner
                lon-corner))))

;; ## MODIS Sampler
;;
;; If a dataset we need for our algorithm happens to be in a
;; projection different from the MODIS products, we need to reproject
;; onto the MODIS grid. NOAA's PREC/L dataset, for example, is
;; arranged in a lat, long grid, at a resolution of 0.5 degrees per
;; pixel.

(defmapcatop [project-to-modis [m-res ascii-info chunk-size]]
  ^{:doc "Accepts a MODIS tile coordinate and a month of PREC/L data
  at `step` resolution on the WGS84 grid, and samples the rain data
  into chunks of pixels in the MODIS sinusoidal projection. The
  function emits a 2-tuple of the form `[chunk-idx chunk-seq]`."}
  [rain-month mod-h mod-v]
  (let [[width] (dimensions-for-step (:step ascii-info))
        numpix (#(* % %) (m/pixels-at-res m-res))
        rdata (vec rain-month)]
    (for [chunk (range (/ numpix chunk-size))
          :let [indexer (comp (partial apply rowcol->idx width)
                              (partial wgs84-indexer m-res ascii-info))
                tpos (partial m/tile-position m-res chunk-size chunk)]]
      [chunk (map rdata
                  (for [pixel (range chunk-size)]
                    (apply indexer mod-h mod-v (tpos pixel))))])))
