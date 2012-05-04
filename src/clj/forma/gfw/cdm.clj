(ns forma.gfw.cdm
  (use [clojure.math.numeric-tower :only (expt)]))

(def tile-size 256)
(def radius 6378137)
(def circumf (* (* 2 Math/PI) radius))
(def initial-res (/ circumf tile-size))
(def origin-shift (/ circumf 2))

(defn lat->y
  [lat]
  (-> (+ 90 lat)
               (* Math/PI)
               (/ 360)
               (Math/tan)
               (Math/log)
               (/ (/ Math/PI 180))
               (* (/ origin-shift 180))))

(defn lon->x
  [lon]
  (/ (* lon origin-shift) 180))

(defn res-at-zoom
  [z]
  (/ initial-res (expt 2 z)))

(defn px-coord
  "Calculate pixel"
  [ms res]
  (-> (+ ms origin-shift)
      (/ res)))

(defn px->tile
  [coord]
  (-> (/ coord tile-size)
      (Math/ceil)
      (- 1)
      (int)))

(defn ty->google-ty
  [ty z]
  (-> (- (expt 2 z) 1)
      (- ty)))

(defn latlon->merc-xy
  "http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/globalmaptiles.py"
  [lat lon]
  [(lon->x lon) (lat->y lat)])

(defn meters->pixels
  "Converts EPSG:900913 to pyramid pixel coordinates in given zoom level"
  [mx my z]
  (let [res (res-at-zoom z)]
    [(px-coord mx res) (px-coord my res)]))

(defn pixels->tile
  "Returns a tile covering region in given pixel coordinates"
  [px py]
  [(px->tile px) (px->tile py)])

(defn meters->tile
  [mx my z]
  (->> (meters->pixels mx my z)
       (apply pixels->tile)))

(defn latlon->tile
  [lat lon z]
  (let [[mx my] (latlon->merc-xy lat lon)]
    (meters->tile mx my z )))

(defn tile->google-tile
  [tx ty z]
  [tx (ty->google-ty ty z)])

(defn latlon->google-tile
  [lat lon z]
  (let [[tx ty] (latlon->tile lat lon z)]
    (tile->google-tile tx ty z)))