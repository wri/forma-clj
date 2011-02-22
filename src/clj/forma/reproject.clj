;; The functions in this namespace allow reprojection of a gridded
;; (lat, lon) dataset at arbitrary resolution into the MODIS
;; sinusoidal grid at arbitrary resolution.

(ns forma.reproject
  (:use cascalog.api
        forma.modis
        (clojure.contrib.generic [math-functions :only
                                  (cos floor abs)])))

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
;; MODIS resolution. When this map in hand, running `(map dataset
;; new--coords)` will sample the old dataset into MODIS at the
;; resolution we want.
;;
;; To parellelize this process effectively, we'll generate maps for
;; chunks of pixels at fixed size, rather than for each tile. If we
;; use the tile level, we have to use one mapper for each tile -- at
;; high resolutions, this becomes inefficient. With "chunks", mappers
;; scale with total number of pixels, which scales directly with
;; spatial resolution.

(defn to-rad [angle] (Math/toRadians angle))
(defn to-deg [angle] (Math/toDegrees angle))

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
  [mod-h mod-v sample line res]
  (apply sinu-latlon
         (map-coords mod-h mod-v sample line (str res))))

;; ### WGS84 -> MODIS Index Mapping
;;
;; Now that we have a method of calculating the wgs84 coordinates of a
;; single MODIS pixel, we're able to consider the problem of
;; generating sampling indices for chunks of MODIS pixels. If we have
;; a dataset in WGS84 gridded at some spatial resolution, to reproject
;; into MODIS we'll need to generate lists of indices that tell us
;; which WGS84 index

;; I'm going to stop in the middle of this, as I know I'm not doing a
;; good job -- but we're almost done, here. The general idea is to
;; take in a rain data month, and generate a list comprehension with
;; every possible chunk at the current resolution. This will stream
;; out a lazy seq of 4 tuples, containing rain data for a given MODIS
;; chunk, only for valid MODIS tiles. If any functions don't make
;; sense, look at reproject.clj. This matches up with stuff over there.

(defn idx->colrow
  "Takes an index within a row vector, and returns the appropriate row
  and column within a square matrix with the supplied edge length."
  [edge idx]
  ((juxt #(mod % edge) #(quot % edge)) idx))

(defn tile-position
  "For a given MODIS chunk and index within that chunk, returns
  [sample, line] within the MODIS tile."
  [m-res chunk index chunk-size]
  (idx->colrow (pixels-at-res m-res)
               (+ index (* chunk chunk-size))))

;; TODO -- rename this, docstring.
(defn index
  "General index -- we use this in lon-index and lat-index below."
  [res x]
  (int (floor (* x (/ res)))))

;; TODO -- rename this. rename in rain-ndex above.
;; Also, get that 720 out of there, using ll-res!
(defn indy
  "Returns the PREC/L dataset coordinates, assuming a 720 x 360
  grid. We get row and column out of this puppy."
  [ll-res lat lon]
  (let [lon-idx (index ll-res (abs lon))
        lat-idx (index ll-res (+ lat 90))]
    (vector lat-idx
            (if (neg? lon)
              (- (dec 720) lon-idx)
              lon-idx))))

;; TODO -- comment, get rid of the 720.
;; TODO -- make sure we can take the MODIS resolution and rain
;; resolution as parameters here.
(defn rain-index
  "takes a modis coordinate, and returns the index within a row vector
  containing a month of PREC/L data at 0.5 resolution."
  [m-res ll-res mod-h mod-v sample line]
  (let [[lat lon] (modis->latlon mod-h mod-v sample line m-res)
        [row col] (indy ll-res lat lon)]
    (+ (* row 720) col)))

;; TODO -- rename this from resample.
;; TODO -- can we just return index, here?  Then, we could have, for
;; an input of chunk size, data, resolution -- we'd actually just need
;; chunk-size and resolution as inputs.  mod-h, mod-v, chunk,
;; chunk-seq. But the chunk-seq would actually be the proper indices
;; within the data!  So the results of this would be a huge business
;; of those four parameters. Every months would need them all.
;;
;;It would take ALL of those and a given month -- and return all of
;; the samples. But we'd be able to split these between everything.
;; TODO -- update docstring.
;; @TODO -- combine this with the defmapcatop below. No reason for them
;; to be separate
(defn resample
  "Takes in a month's worth of PREC/L rain data, and returns a lazy
  seq of data samples for supplied MODIS chunk coordinates."
  [m-res ll-res mod-h mod-v chunk-size chunk]
  (for [pixel (range chunk-size)
        :let [[sample line] (tile-position m-res chunk pixel chunk-size)]]
    (rain-index m-res ll-res mod-h mod-v sample line)))

;; This guy samples each chunk. I think we can actually just call this
;; particular function with a huge chunk size, if we want to do each tile.
(defmapcatop [chunk-samples [m-res ll-res chunk-size]]
  ^{:doc "Returns chunks of the indices within a lat lon array at the
  specified resolution required to match up with modis chunks of the
  supplied size, within the tile at the supplied MODIS coordinates."}
  [mod-h mod-v]
  (let [edge (pixels-at-res m-res)
        numpix (#(* % %) edge)]
    (for [chunk (range (/ numpix chunk-size))]
      [chunk (resample m-res ll-res mod-h mod-v chunk-size chunk)])))