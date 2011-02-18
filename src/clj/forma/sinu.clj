;; TODO -- talk about how this section essentially takes arc out of
;; the picture, since we can now generate maps between any ordered
;; lat, long data and the MODIS grid, at the supplied resolutions.

;; ## MODIS Reprojection
;;
;; If a dataset we need for our algorithm happens to be in a
;; projection different than the MODIS products, we need to reproject
;; that data pixel coordinates on the MODIS grid. NOAA's PREC/L
;; dataset, for example, is arranged in a lat, long grid, at a
;;resolution of 0.5 degrees per pixel.
;;;
;;each pixel is 0.5
;; degrees on a side. Back in python, we used a pregenerated set of
;; indices for every MODIS tile. When we run `(map dataset
;; new-coords)`, we obtain a vector of ordered rain data for every
;; pixel within that particular MODIS tile.
;;
;; This worked well on one machine; in the original python version of
;; the code, we load every bit of rain data into one massive array,
;; and then cycle through the indices, one tile at a time.
;;
;; Parallelizing this process presented some issues. How would we
;; split up the data? Because we can't know in advance which portion
;; the rain data array we're going to need, we either need to:
;;
;; 1. Load every rain array onto each cluster, and map across
;;pregenerated indices;
;; 1. load all (TODO -- note that this file is all about resampling
;;and reprojection. We're able to generate our chunk samples, here.
;; 1. load all
;;
;; of the rain data onto each node in the cluster, 

;; Now, as referenced here:
;; http://www.dfanning.com/map_tips/modis_overlay.html this os what
;; NASA stopped using integerized sinusoidal projection in collection
;; 3, and moved on to strictly sinusoidal in collection 4. I believe
;; that NDVI uses a straight up sinusoidal, so that's all we need to
;; worry about.

;;TODO -- document, inside of these functions, what a sinusoidal
;;projection actually is, with some links to understanding the general
;;idea behind all of this stuff. The code here recreates the
;;functionality found at the MODLAND Tile Calculator:
;;http://landweb.nascom.nasa.gov/developers/tilemap/note.html.

(ns forma.sinu
  (:use cascalog.api
        (clojure.contrib.generic [math-functions :only
                                  (cos floor abs)])))

;; ## Inverse Sinusoidal Projection

;; ## Constants

(def rho 6371007.181)
(def h-tiles 36)
(def v-tiles 18)

(def
  #^{:doc "Set of coordinate pairs for all MODIS tiles that contain
actual data. This set is calculated by taking a vector of offsets,
representing the first horizontal tile containing data for each row of
tiles. (For example, the data for row 1 begins with tile 14,
horizontal.)  For a visual representation of the MODIS grid and its
available data, see http://remotesensing.unh.edu/modis/modis.shtml"}
  good-tiles
  (let [offsets [14 11 9 6 4 2 1 0 0 0 0 1 2 4 6 9 11 14]]
    (vec (for [v-tile (range v-tiles)
               h-tile (let [shift (offsets v-tile)]
                        (range shift (- h-tiles shift)))]
           [h-tile v-tile]))))

(def pixels-at-res
  {"250" 4800
   "500" 2400
   "1000" 1200})

;; ## Helper Functions

(defn to-rad [angle] (Math/toRadians angle))
(defn to-deg [angle] (Math/toDegrees angle))

(defn distance
  "Calculates distance of magnitude from the starting point in a given
  direction."
  [dir start magnitude]
  (dir start magnitude))

(defn scale
  "Scales each element in a collection of numbers by the supplied
  factor."
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
  "Computes the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in radians)."
  [lat lon]
  (scale rho [(* (cos lat) lon) lat]))

(defn sinu-deg-xy
  "Computes the sinusoidal x and y coordinates for the supplied
  latitude and longitude (in degrees)."
  [lat lon]
  (apply sinu-xy (map to-rad [lat lon])))

;; These are the meter values of the minimum possible x and y values
;; on a sinusoidal projection. Things get weird at the corners, so I
;; compute the minimum x and y values separately, along the
;; projection's axes.

(def min-x (x-coord (sinu-deg-xy 0 -180)))
(def min-y (y-coord (sinu-deg-xy -90 0)))
(def max-y (y-coord (sinu-deg-xy 90 0)))

;; ## Conversion Functions

(defn lat-long
  "Computes the latitude and longitude for a given set of sinusoidal
  map coordinates (in meters)."
  [x y]
  (let [lat (/ y rho)
        lon (/ x (* rho (cos lat)))]
    (map to-deg [lat lon])))

(defn pixel-length
  "The length, in meters, of the edge of a pixel at a given
  resolution."
  [res]
  (let [pixel-span (/ (- max-y min-y) v-tiles)
        total-pixels (pixels-at-res res)]
    (/ pixel-span total-pixels)))

;; ## Main Conversion Functions

(defn pixel-coords
  "returns the row and dimension of the pixel on the global MODIS grid."
  [mod-h mod-v sample line res]
  (let [edge-pixels (pixels-at-res res)]
    (map + [sample line] (scale edge-pixels [mod-h mod-v]))))

(defn map-coords
  "Returns the map position in meters for a given MODIS tile
  coordinate at the specified resolution."
  [mod-h mod-v sample line res]
  (let [edge-length (pixel-length res)
        half-edge (/ edge-length 2)
        pix-pos (pixel-coords mod-h mod-v sample line res)
        magnitudes (map #(+ half-edge %) (scale edge-length pix-pos))]
    (map distance [+ -] [min-x max-y] magnitudes)))

(defn geo-coords
  "Returns the latitude and longitude for a given group of MODIS tile
  coordinates at the specified resolution."
  [mod-h mod-v sample line res]
  (apply lat-long
         (map-coords mod-h mod-v sample line (str res))))

;; ## Resampling of Rain Data
;; I'm going to stop in the middle of this, as I know I'm not doing a
;; good job -- but we're almost done, here. The general idea is to
;; take in a rain data month, and generate a list comprehension with
;; every possible chunk at the current resolution. This will stream
;; out a lazy seq of 4 tuples, containing rain data for a given MODIS
;; chunk, only for valid MODIS tiles. If any functions don't make
;; sense, look at sinu.clj. This matches up with stuff over there.

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
  (let [[lat lon] (geo-coords mod-h mod-v sample line m-res)
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
;; to be separate@
(defn resample
  "Takes in a month's worth of PREC/L rain data, and returns a lazy
  seq of data samples for supplied MODIS chunk coordinates."
  [m-res ll-res mod-h mod-v chunk chunk-size]
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
      [chunk
       (vec (resample m-res ll-res mod-h mod-v chunk chunk-size))])))