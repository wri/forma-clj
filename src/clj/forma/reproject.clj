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
;;
;; In their current version, these functions make the assumption that
;; the WGS84 grid begins at (-90, 0), with columns moving east and
;; rows moving north. This goes against the MODIS convention of
;; beginning at (-180, 90), and moving east and south. Future versions
;; will accomodate arbitrary zero-points.

(defn bucket
  "Takes a floating-point value and step size, and returns the
  step-sized bucket into which the value falls. For example:

     (bucket 0.4 1.3)
     ;=> 3

     (bucket 0.9 1.3)
     ;=> 1"
  [step val]
  (->> step (/ 1) (* val) Math/floor int))

(defn dimensions-for-step
  "returns the <horz, vert> dimensions of a WGS84 grid with the
  supplied spatial step between pixels."
  [step]
  (map #(quot % step) [360 180]))

(defn line-torus
  "TODO: Docstring and rename."
  [dir range corner val]
  (let [[min max] (if (= dir -)
                    [corner (dir corner range)]
                    [(dir corner range) corner])]
    (loop [out val]
      (cond (< out max) (recur (+ out range))
            (> out min) (recur (- out range))  
            :else (- out corner)))))

(defn fit-to-grid
  "Takes a coordinate pair and returns its [row, col] position on a
  WGS84 grid with the supplied spatial resolution and width in
  columns."
  [step lat-dir lon-dir lat-corner lon-corner lat lon]
  (map (partial bucket step)
       [(line-torus lat-dir 180 lat-corner lat)
        (line-torus lon-dir 360 lon-corner lon)]))

(defn wgs84-indexer
  "Generates a function that accepts MODIS tile coordinates and
  returns the corresponding `[row, col]` within a WGS84 grid of values
  with the supplied step-size, corner coordinates and directions
  traveled along each axis."
  [m-res step lat-dir lon-dir lat-corner lon-corner]
  (fn [mod-h mod-v sample line]
    {:pre [(m/valid-modis? m-res mod-h mod-v sample line)]}
    (->> (m/modis->latlon m-res mod-h mod-v sample line)
         (apply fit-to-grid step lat-dir lon-dir lat-corner
                lon-corner))))

;; ## MODIS Sampler
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
;;
;; We made the decision to take a single MODIS tile id and a month of
;; rain data as dynamic variables, against my earlier idea of
;; pregenerating indices for each chunk, and pairing the rain months
;; up against these. The idea of pregenerating indices was spurred by
;; the thought that the sampling process, applied at the tile level,
;; wouldn't scale well as resolution increased. This is still true;
;; for high resolutions, this function takes on a rather heavy
;; burden. Additionally, with this current method, we generate the
;; sampling indices for every tile & rain-data combination.
;;
;; Still, we decided to stay at the tile level, as the tuple blowup
;; caused by pairing each rain-month with chunks of 24,000 was
;; enormous. Each of these methods requires a cross join between
;; rain-months and some dataset, either tiles or the chunk indices,
;; and a cross join has to funnel through a single reducer. The
;; intermediate data for a chunk-size of 24,000 at 250m resolution
;; would have produced 2TB of intermediate data.
;;
;; If it becomes clear how to send a `(* 1200 1200)` vector out of
;; this function, one way to guarantee efficiency here would be to
;; pregenerate indices, but choose a chunk-size of `(* 1200 1200)`,to
;; produce a single chunk for 1km tiles. 250 meter data, with 16x the
;; pixels, would run at the same speed with 16x the machines.

(defmapcatop [project-to-modis [m-res step chunk-size]]
  ^{:doc "Accepts a MODIS tile coordinate and a month of PREC/L data
  at `step` resolution on the WGS84 grid, and samples the rain data
  into chunks of pixels in the MODIS sinusoidal projection. The
  function emits a 2-tuple of the form `[chunk-idx chunk-seq]`.

  WARNING: Handles one tile at a time. Not good for 250m data!"}
  [rain-month mod-h mod-v]
  (let [[width] (dimensions-for-step step)
        numpix (#(* % %) (m/pixels-at-res m-res))
        rdata (vec rain-month)]
    (for [chunk (range (/ numpix chunk-size))
          :let [indexer (comp (partial apply rowcol->idx width)
                              (wgs84-indexer m-res step + + -90 0))
                tpos (partial m/tile-position m-res chunk-size chunk)]]
      [chunk (map rdata
                  (for [pixel (range chunk-size)]
                    (apply indexer mod-h mod-v (tpos pixel))))])))
