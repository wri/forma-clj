;; The functions in this namespace allow reprojection of a gridded
;; (lat, lon) dataset at arbitrary resolution into the MODIS
;; sinusoidal grid at arbitrary resolution.

(ns forma.reproject
  (:use [forma.matrix.utils :only (idx->rowcol)])
  (:require [forma.utils :as u]))

;; From the [user's guide](http://goo.gl/uoi8p) to MODIS product MCD45
;; (burned area): "The MODIS data are re-projected using an equiareal
;; sinusoidal projection, defined on a sphere of radius 6371007.181 m,
;; and with the Greenwich meridian as the central meridian of the
;; projection." The full MODIS grid has 18 vertical and 36 horizontal
;; tiles. Each tile is subdivided into pixels, based on the dataset's
;; spatial resolution.

(def rho 6371007.181)

(def h-tiles 36)
(def v-tiles 18)

(def pixels-at-res
  {"250" 4800
   "500" 2400
   "1000" 1200})

(defn temporal-res
  "Returns the temporal resolution of the supplied MODIS short product
  name."
  [dataset]
  (case dataset
        "MCD45A1" "32"
        "MOD13Q1" "16"
        "MOD13A1" "16"
        "MOD13A2" "16"
        "MOD13A3" "32"))

(defn chunk-dims
  "Returns the width and height in pixels of a chunk at the supplied
  resolution with `chunk-size` total entries.

  `chunk-size` must be a whole number multiple of the number of pixels
  per row in a MODIS tile of the supplied resolution `m-res`."
  [m-res chunk-size]
  {:pre [(string? m-res)
         (pos? chunk-size)
         (zero? (mod chunk-size (pixels-at-res m-res)))]}
  (let [width (pixels-at-res m-res)
        height (quot chunk-size width)]
    [width height]))

(defn wgs84-resolution
  "Returns the step size on a `<lat, lon>` grid corresponding to the
  supplied MODIS resolution. As each MODIS tile is 10 degrees on a
  side, we find this value by calculating the number of degrees
  spanned by each MODIS pixel. This is not exact, as MODIS uses a
  sinusoidal projection, but it's close enough for rough estimation."
  [res]
  (/ 10. (pixels-at-res res)))

(def
  #^{:doc "Set of coordinate pairs for all MODIS tiles that contain
actual data. This set is calculated by taking a vector of offsets,
representing the first horizontal tile containing data for each row of
tiles. (For example, the data for row 1 begins with tile 14,
horizontal.)  For a visual representation of the MODIS grid and its
available data, see http://remotesensing.unh.edu/modis/modis.shtml"}
  valid-tiles
  (let [offsets [14 11 9 6 4 2 1 0 0 0 0 1 2 4 6 9 11 14]]
    (set (for [v-tile (range v-tiles)
               h-tile (let [shift (offsets v-tile)]
                        (range shift (- h-tiles shift)))]
           [h-tile v-tile]))))

(defn valid-modis?
  "Checks that the supplied values correspond to a valid MODIS tile,
  at the specified resolution. The single argument version accepts a
  sequence if `[mod-h, mod-v`] vectors."
  ([tileseq]
     (reduce #(and %1 %2)
             (map (fn [[h v]]
                    (valid-modis? h v))
                  tileseq)))
  ([mod-h mod-v]
     (contains? valid-tiles [mod-h mod-v]))
  ([res mod-h mod-v sample line]
     (let [edge (pixels-at-res res)]
       (and (valid-modis? mod-h mod-v)
            (< sample edge)
            (< line edge)))))

(defn hv->tilestring
  "Returns a 0-padded tilestring of format `HHHVVV`, for the supplied
  MODIS h and v coordinates. For example:

     (tilestring 8 6)
     ;=> \"008006\""
  [mod-h mod-v]
  {:pre [(valid-modis? mod-h mod-v)]}
  (apply str (map (partial format "%03d")
                  [mod-h mod-v])))

(defn tilestring->hv
  "Extracts integer representations of the MODIS H and V coordinates
referenced by the supplied MODIS tilestring, of format 'HHHVVV'."
  [tilestr]
  {:post [(valid-modis? [%])]}
  (map (comp #(Integer. %)
             (partial apply str))
       (partition 3 tilestr)))

(defn tile-position
  "General version accepts a window's dimensions and position and a
pixel index within that window, and returns the global sample and
line. For example:

    (tile-position 10 10 2 1 0) => [20 10]

  Special-case version accepts a MODIS resolution, a chunk-size, and a
  pixel's index within that chunk, and returns the tile sample and
  line. For example:

    (tile-position \"1000\" 24000 2 1231) => [31 41]"
  ([m-res chunk-size chunk-row idx]
     (let [[w h] (chunk-dims m-res chunk-size)]
       (tile-position w h 0 chunk-row idx)))
  ([win-width win-height win-col win-row idx]
     {:pre [(not (or (neg? win-col) (neg? win-row)))]}
     (let [[row col] (idx->rowcol win-height win-width idx)]
       [(+ col (* win-width win-col))
        (+ row (* win-height win-row))])))

;; ### Spherical Sinusoidal Projection
;;
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

(defn valid-latlon? [lat lon]
  (and (u/between? -90 90 lat)
       (u/between? -180 180 lon)))

(defn latlon-rad->sinu-xy
  "Returns the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in radians)."
  [lat lon]
  (u/scale rho [(* (Math/cos lat) lon) lat]))

(defn latlon->sinu-xy
  "Returns the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in degrees)."
  [lat lon]
  {:pre [(valid-latlon? lat lon)]}
  (apply latlon-rad->sinu-xy
         (map #(Math/toRadians %) [lat lon])))

;; ### Inverse Sinusoidal Projection
;;
;; We'll stay in degrees, here. To convert from MODIS to (lat, lon),
;; we need some way to convert distance from the MODIS origin (tile
;; `[0 0]`, pixel `[0 0]`) in pixels into distance from the sinusoidal
;; origin (`[0 0]` in meters, corresponding to (lat, lon) = `[-90
;; -180]`).

(defn sinu-xy->latlon
  "Returns the latitude and longitude (in degrees) for a given set of
  sinusoidal map coordinates (in meters)."
  [x y]
  (let [lat (/ y rho)
        lon (/ x (* rho (Math/cos lat)))]
    (map #(Math/toDegrees %) [lat lon])))

;; These are the meter values of the minimum possible x and y values
;; on a sinusoidal projection. The sinusoidal projection is quite
;; deformed at the corners, so we compute the minimum x and y values
;; separately, at the ends of the projection's axes.

(defn x-coord [point] (first point))
(defn y-coord [point] (second point))

(def min-x (x-coord (latlon->sinu-xy 0 -180)))
(def min-y (y-coord (latlon->sinu-xy -90 0)))
(def max-y (y-coord (latlon->sinu-xy 90 0)))

(defn pixel-length
  "The length, in meters, of the edge of a MODIS pixel at the supplied
  resolution."
  [res]
  (let [pixel-span (/ (- max-y min-y) v-tiles)
        total-pixels (pixels-at-res res)]
    (/ pixel-span total-pixels)))

(defn global-mags->sinu-xy
  "Returns the coordinate position on a sinusoidal grid reached after
  traveling the supplied magnitudes in the x and y directions. The
  origin of the sinusoidal grid is fixed in the top left corner."
  [mag-x mag-y]
  (map #(%1 %2 %3) [+ -] [min-x max-y] [mag-x mag-y]))

(defn sinu-xy->global-mags
  "Returns the magnitudes (in meters) required to reach the supplied
  coordinate position on a the MODIS sinusoidal grid. The origin of
  the sinusoidal grid is fixed in the top left corner."
  [x y]
  (map #(%1 %2 %3) [- -] [x max-y] [min-x y]))

(defn modis->global-mags
  "Returns the distance traveled in meters from the top left of the
  sinusoidal MODIS grid that corresponds with the supplied MODIS pixel
  coordinates at the given resolution"
  [res mod-h mod-v sample line]
  (let [edge-length (pixel-length res)
        edge-pixels (pixels-at-res res)]
    (->> [mod-h mod-v]
         (u/scale edge-pixels)
         (map + [sample line])
         (u/scale edge-length)
         (map #(+ % (/ edge-length 2))))))

(defn global-mags->modis
  "Returns the MODIS pixel coordinate reached after traveling the
  supplied meter distances in the positive X and negative Y directions
  from the origin of the sinusoidal MODIS grid, located at its top
  left."
  [res mag-x mag-y]
  (let [edge-length (pixel-length res)
        edge-pixels (pixels-at-res res)
        [tile-h sample tile-v line]
        (mapcat (comp
                 (juxt #(quot % edge-pixels)
                       #(mod % edge-pixels))
                 #(-> % (* (/ edge-length)) int))
                [mag-x mag-y])]
    [tile-h tile-v sample line]))

;; See [this gist](https://gist.github.com/939337) for an example of a
;; way to attack this pattern with a macro.

(defn modis->latlon
  "Converts the supplied MODIS coordinates into `[lat, lon]` based on
  the supplied resolution.

Example usage:

    (modis->latlon \"1000\" 8 6 12 12)
    ;=> (29.89583333333333 -115.22901262147285)"
  [res mod-h mod-v sample line]
  (->> (modis->global-mags res mod-h mod-v sample line)
       (apply global-mags->sinu-xy)
       (apply sinu-xy->latlon)))

(defn latlon->modis
  "Converts the supplied latitude and longitude into MODIS pixel
  coordinates at the supplied resolution.

Example usage:

    (latlon->modis \"1000\" 29.89583 -115.2290)
    ;=> [8 6 12 12]"
  [modis-res lat lon]
  (->> (latlon->sinu-xy lat lon)
       (apply sinu-xy->global-mags)
       (apply global-mags->modis modis-res)))

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
    (and (>= row 0) (<= row rows)
         (>= col 0) (<= col cols))))

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
         (apply latlon->modis m-res))))

(defn wgs84-indexer
  "Accepts MODIS tile coordinates and returns the corresponding `[row,
  col]` within a WGS84 grid of values with the supplied step-size,
  corner coordinates and directions traveled for each axis."
  [m-res ascii-map mod-h mod-v sample line]
  {:pre [(valid-modis? m-res mod-h mod-v sample line)]}
  (let [{:keys [step corner travel]} ascii-map
        [lon-corner lat-corner] corner
        [lon-dir lat-dir] travel]
    (->> (modis->latlon m-res mod-h mod-v sample line)
         (apply latlon->rowcol step
                lat-dir lon-dir
                lat-corner lon-corner))))
