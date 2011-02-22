;; The functions in this namespace allow reprojection of a gridded
;; (lat, lon) dataset at arbitrary resolution into the MODIS
;; sinusoidal grid at arbitrary resolution.

(ns forma.reproject
  (:use cascalog.api
        forma.modis
        (clojure.contrib.generic [math-functions :only
                                  (cos floor abs sqr)])))

;; ## MODIS Reprojection
;;
;; ### Spherical Sinusoidal Projection
;;
;; If a dataset we need for our algorithm happens to be in a
;; projection different from the MODIS products, we need to reproject
;; onto the MODIS grid. NOAA's PREC/L dataset, for example, is
;; arranged in a lat, long grid, at a resolution of 0.5 degrees per
;; pixel.
;;
;; To resample in parallel, we decided to pre-generate a set of array
;; indices, to map from a specific (lat, lon) resolution into some
;; MODIS resolution. When this map in hand, running
;;
;;     (map dataset new-coords)
;;
;; will sample the old dataset into MODIS at the resolution we want.
;;
;; To parellelize this process effectively, we'll generate maps for
;; chunks of pixels at fixed size, rather than for each tile. If we
;; use the tile level, we have to use one mapper for each tile -- at
;; high resolutions, this becomes inefficient. With "chunks", mappers
;; scale with total number of pixels, which scales directly with
;; spatial resolution.

(defn to-rad [angle] (Math/toRadians angle))
(defn to-deg [angle] (Math/toDegrees angle))

(defn dimensions-at-res
  "returns the <horz, vert> dimensions of a WGS84 grid at the supplied
  spatial resolution."
  [res]
  (map #(quot % res) [360 180]))

(defn scale
  "Scales each element in a collection of numbers by the supplied
  factor."
  [fact sequence]
  (for [x sequence] (* x fact)))

;; From [Wikipedia](http://goo.gl/qG7Hi), "the sinusoidal projection
;; is a pseudocylindrical equal-area map projection, sometimes called
;; the Sanson-Flamsteed or the Mercator equal-area projection. It is
;; defined by:
;;
;; $$x = (\lambda - \lambda_{0}) \cos \phi$$
;; $$y = \phi$$
;;
;; where \\(\phi\\) is the latitude, \\(\lambda\\) is the longitude,
;; and \\(\lambda_{0}\\) is the central meridian." (All angles are
;; defined in radians.) The central meridian of the MODIS sinusoidal
;; projection is 0 (as shown in the MODIS [WKT
;; file](http://goo.gl/mXIaY)), so the equation for \\(x\\) can be
;; simplified to
;;
;; $$x = \lambda \cos \phi$$
;;
;; MODIS models the earth as a sphere with radius \\(\rho =
;; 6371007.181\\), requiring us to scale all calculations by
;; \\(\rho\\).
;;
;; This application deals primarily with the inverse problem, of
;; converting (x, y) back into latitude and longitude. Still, we have
;; to define the forward equations, for the purpose of computing the
;; minimum and maximum possible x and y values. The MODIS grid
;; partitions the sinusoidal grid into an arbitrary number of pixels;
;; we need these values to appropriately map between meters and pixels
;; on the subdivided grid.

(defn sinu-xy
  "Returns the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in radians)."
  [lat lon]
  (scale rho [(* (cos lat) lon) lat]))

(defn sinu-deg-xy
  "Returns the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in degrees)."
  [lat lon]
  (apply sinu-xy (map to-rad [lat lon])))

;; These are the meter values of the minimum possible x and y values
;; on a sinusoidal projection. The sinusoidal projection is quite
;; deformed at the corners, , so we compute the minimum x and y values
;; separately, at the ends of the projection's axes.

(defn x-coord [point] (first point))
(defn y-coord [point] (second point))

(def min-x (x-coord (sinu-deg-xy 0 -180)))
(def min-y (y-coord (sinu-deg-xy -90 0)))
(def max-y (y-coord (sinu-deg-xy 90 0)))

;; ### Inverse Sinusoidal Projection
;;
;; Now, back to the inverse problem. We'll stay in degrees, here. To
;; convert from MODIS to (lat, lon), we need some way to convert
;; distance from the MODIS origin (tile [0 0], pixel [0 0]) in pixels
;; into distance from the sinusoidal origin ([0 0] in meters,
;; corresponding to (lat, lon) = [-90 -180]).

(defn sinu-latlon
  "Returns the latitude and longitude (in degrees) for a given set of
  sinusoidal map coordinates (in meters)."
  [x y]
  (let [lat (/ y rho)
        lon (/ x (* rho (cos lat)))]
    (map to-deg [lat lon])))

(defn sinu-position
  "Returns the coordinate position on a sinusoidal grid reached after
  traveling the supplied magnitudes in the x and y directions. The
  origin of the sinusoidal grid is fixed in the top left corner."
  [mag-x mag-y]
  (letfn [(travel [dir start magnitude]
                  (dir start magnitude))]
    (map travel [+ -] [min-x max-y] [mag-x mag-y])))

(defn pixel-length
  "The length, in meters, of the edge of a MODIS pixel at the supplied
  resolution."
  [res]
  (let [pixel-span (/ (- max-y min-y) v-tiles)
        total-pixels (pixels-at-res res)]
    (/ pixel-span total-pixels)))

(defn pixel-coords
  "Returns the (sample, line) of a pixel on the global MODIS grid,
  measured in pixels from (0,0) at the top left."
  [mod-h mod-v sample line res]
  (let [edge-pixels (pixels-at-res res)]
    (map + [sample line] (scale edge-pixels [mod-h mod-v]))))

(defn map-coords
  "Returns the map position in meters of the supplied MODIS tile
  coordinate at the specified resolution."
  [mod-h mod-v sample line res]
  (let [edge-length (pixel-length res)
        half-edge (/ edge-length 2)
        pix-pos (pixel-coords mod-h mod-v sample line res)
        magnitudes (map #(+ half-edge %) (scale edge-length pix-pos))]
    (apply sinu-position magnitudes)))

(defn modis->latlon
  "Returns the latitude and longitude for the supplied MODIS tile
  coordinate at the specified resolution."
  [res mod-h mod-v sample line]
  (apply sinu-latlon
         (map-coords mod-h mod-v sample line (str res))))

;; ### WGS84 -> MODIS Index Mapping
;;
;; Now that we have a method of calculating the wgs84 coordinates of a
;; single MODIS pixel, we're able to consider the problem of
;; generating sampling indices for chunks of MODIS pixels. If we have
;; a dataset in WGS84 gridded at some spatial resolution, to reproject
;; into MODIS we'll need to generate lists that tell us which WGS84
;; indices to sample for the data at each MODIS pixel. (The following
;; assume that WGS84 data is held in a row vector.)
;;
;; In their current version, these functions make the assumption that
;; the WGS84 grid begins at (-90, 0), with columns moving east and
;; rows moving north. This goes against the MODIS convention of
;; beginning at (-180, 90), and moving east and south. Future versions
;; will accomodate arbitrary zero-points.

(defn idx->colrow
  "Takes an index within a row vector, and returns the appropriate
  column and row within a matrix with the supplied dimensions. If only
  one dimension is supplied, assumes a square matrix."
  ([edge idx]
     (idx->colrow edge edge idx))
  ([width height idx]
     {:pre [(< idx (* width height)), (not (neg? idx))]
      :post [(and (< (first %) width)
                  (< (second %) height))]}
     ((juxt #(mod % width) #(quot % width)) idx)))

(defn colrow->idx
  "For the supplied column and row in a rectangular matrix of
dimensions (height, width), returns the corresponding index within a
row vector of size (* width height). If only one dimension is
supplied, assumes a square matrix."
  ([edge col row]
     (colrow->idx edge edge col row))
  ([width height col row]
     {:post [(< % (* width height))]}
     (+ col (* width row))))

(defn tile-position
  "For a given MODIS chunk and index within that chunk, returns
  [sample, line] within the MODIS tile."
  [m-res chunk-size chunk index]
  (idx->colrow (pixels-at-res m-res)
               (+ index (* chunk chunk-size))))

(defn bucket
  "Takes a floating-point value and step size, and returns the
  step-sized bucket into which the value falls. For example:

      (index 0.4 1.3)
      ;=> 3

      (index 0.9 1.3)
      ;=> 1"
  [step val]
  (int (floor (* val (/ step)))))

(defn fit-to-grid
  "Takes a coordinate pair and returns its [row, col] position on a
  WGS84 grid with the supplied spatial resolution and width in
  columns.

 (`fit-to-grid` assumes that the WGS84 grid begins at -90 latitude and
  0 longitude. Columns move east, wrapping around the globe, and rows
  move north.)"
  [ll-res max-width lat lon]
  (let [lon-idx (bucket ll-res (abs lon))
        lat-idx (bucket ll-res (+ lat 90))]
    (vector lat-idx
            (if (neg? lon)
              (- (dec max-width) lon-idx)
              lon-idx))))

(defn wgs84-index
  "takes a modis coordinate at the supplied resolution, and returns
  the index within a row vector of WGS84 data at the supplied
  resolution."
  [m-res ll-res mod-h mod-v sample line]
  {:pre [(valid-modis? m-res mod-h mod-v sample line)]}
  (let [[max-width] (dimensions-at-res ll-res)
        [lat lon] (modis->latlon m-res mod-h mod-v sample line)
        [row col] (fit-to-grid ll-res max-width lat lon)]
    (colrow->idx max-width col row)))

(defmapcatop [chunk-samples [m-res ll-res chunk-size]]
  ^{:doc "Returns chunks of the indices within a WGS84 array at the
  specified resolution corresponding to MODIS chunks of the supplied
  size, within the tile at the supplied MODIS coordinates."}
  [mod-h mod-v]
  (let [edge (pixels-at-res m-res)
        numpix (sqr edge)
        indexer (partial wgs84-index m-res ll-res mod-h mod-v)]
    (for [chunk (range (/ numpix chunk-size))]
      [chunk
       (map #(apply indexer (tile-position m-res chunk-size chunk %))
            (range chunk-size))])))