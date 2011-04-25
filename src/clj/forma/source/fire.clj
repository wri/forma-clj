(ns forma.source.fire
  (:use cascalog.api
        [forma.matrix.utils :only (idx->colrow)]
        [forma.reproject :only (wgs84-index
                                dimensions-at-res)]
        [forma.source.modis :only (latlon->modis pixels-at-res)]
        [clojure.string :only (split)])
  (:require [cascalog.ops :as c]))

(def fire-tap
  (memory-source-tap
   [["455888,30,A,-12.687,21.323,312.8,287.1,198,47.5,80"]
    ["455889,30,A,-14.141,25.583,319.8,288,571,24.7,90"]
    ["455890,30,A,-14.142,25.592,318.1,288.7,572,22.6,77"]
    ["455891,30,A,-14.159,25.58,308.9,288.8,571,12.6,19"]
    ["455892,30,A,-14.161,25.589,308.6,288.9,572,12.3,47"]
    ["455893,30,A,-14.171,25.597,331.4,289.8,573,39.7,100"]
    ["455894,30,A,-14.172,25.607,325.8,289.1,574,32.2,100"]
    ["455895,30,A,-15.288,29.643,313.5,289.8,995,26.3,82"]
    ["455896,30,A,-15.872,30.327,305.7,289.8,1057,18.1,36"]
    ["455897,30,A,-25.555,29.305,311,271.8,1117,47.8,73"]]))

(defn mangle
  "Rips apart lines in a fires dataset."
  [line]
  (map (fn [val]
         (try (Float. val)
              (catch Exception _
                (read-string val))))
       (split line #",")))

(defn fire-source
  "Takes a source of textlines, and returns 2-tuples with latitude and
  longitude."
  [source]
  (<- [?lat ?lon]
      (source ?line)
      (mangle ?line :> _ _ _ ?lat ?lon _ _ _ _ _)))

(defn rip-fires
  "Aggregates fire data at the supplied path by modis pixel at the
  supplied resolution."
  [m-res path]
  (let [fires (fire-source (hfs-textline path))]
    (<- [?mod-h ?mod-v ?sample ?line ?num-fires]
        (fires ?lat ?lon)
        (latlon->modis m-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (c/count ?num-fires))))

;; ## ASCII Grid Sampling

(defn liberate
  "Takes a line with an index as the first value and numbers as the
  rest, and converts it into a 2-tuple formatted as `[idx, row-vals]`,
  where `row-vals` are sealed inside an `int-array`.

Example usage:

    (liberate \"1 12 13 14 15\")
    ;=> [1 #<int[] [I@1b66e87>]"
  [line]
  (let [[idx & row-vals] (map #(Integer. %)
                              (split line #" "))]
    [idx (int-array row-vals)]))

;; This guy splits apart some particular row, and returns a 2-tuple
;; with the index within the row as the first item.
(defmapcatop free-cols
  [row]
  (map-indexed vector row))

(defn grid-source
  "Takes some source of textlines -- (hfs-textline \"/some/file.csv\",
  for example -- and generates tuples with `row`, `col` and `val` at
  the specified location."
  [source]
  (<- [?row-idx ?col-idx ?val]
      (source ?line)
      (liberate ?line :> ?row-idx ?row-array)
      (free-cols ?row-array :> ?col-idx ?val)))

(defmapcatop
  ^{:doc "Returns tuples containing modis coordinates, plus the
  corresponding row and column of a value within a wgs84 grid at the
  specified spatial resolution. ll-res is the step size of the ASCII
  grid."}
  [sample-modis [m-res ll-res]]
  [mod-h mod-v]
  (let [pix (pixels-at-res m-res)
        [ll-width ll-height] (dimensions-at-res ll-res)]
    (for [sample (range pix)
          line   (range pix)
          :let [idx (wgs84-index m-res ll-res mod-h
                                 mod-v sample line)
                [col row] (idx->colrow ll-width ll-height idx)]]
      [sample line row col])))

(defn sample-ascii
  "Subquery to sample an ascii grid of information at the supplied
  MODIS and wgs84 resolutions. `m-res` is \"1000\", \"500\" or
  \"250\". `ll-res` is the step size on the latitude-longitude grid.

Example usage:

    (?- (stdout)
        (sample-ascii \"1000\" 0.1 \"/path/to/ascii.csv\" [[8 6] [12 10]]))"
  [m-res ll-res ascii-path tile-seq]
  (let [tile-source (memory-source-tap tile-seq)
        ascii-source (grid-source (hfs-textline ascii-path))]
    (<- [?mod-h ?mod-v ?sample ?line ?val]
        (tile-source ?mod-h ?mod-v)
        (sample-modis [m-res ll-res] ?mod-h ?mod-v :> ?sample ?line ?row ?col)
        (ascii-source ?row ?col ?val))))

(defn run-sample-ascii
  "Runs the above subquery, as in `sample-ascii` example."
  [m-res ll-res ascii-path tile-seq]
  (?- (stdout)
      (sample-ascii m-res ll-res
                    ascii-path tile-seq)))
