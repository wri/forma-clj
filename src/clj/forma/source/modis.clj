(ns forma.source.modis
  (:use [forma.matrix.utils :only (idx->colrow)]
        [clojure.contrib.generic.math-functions :only (cos)]))

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

(def temporal-res
  {"MCD45A1" "32"
   "MOD13Q1" "16"
   "MOD13A1" "16"
   "MOD13A2" "16"
   "MOD13A3" "32"})

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
  (apply str (map (partial format "%03d")
                  [mod-h mod-v])))

(defn tilestring->hv
  "Extracts integer representations of the MODIS H and V coordinates
referenced by the supplied MODIS tilestring, of format 'HHHVVV'."
  [tilestr]
  (map (comp #(Integer. %)
             (partial apply str))
       (partition 3 tilestr)))

(defn tile-position
  "For a given MODIS chunk and index within that chunk, returns
  [sample, line] within the MODIS tile."
  [m-res chunk-size chunk index]
  (idx->colrow (pixels-at-res m-res)
               (+ index (* chunk chunk-size))))

;; ### Spherical Sinusoidal Projection

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

(defn latlon-rad->sinu-xy
  "Returns the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in radians)."
  [lat lon]
  (scale rho [(* (cos lat) lon) lat]))

(defn latlon->sinu-xy
  "Returns the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in degrees)."
  [lat lon]
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
        lon (/ x (* rho (cos lat)))]
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
         (scale edge-pixels)
         (map + [sample line])
         (scale edge-length)
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
