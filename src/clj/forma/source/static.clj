(ns forma.source.static
  (:use cascalog.api
        [forma.matrix.utils :only (idx->colrow)]
        [forma.reproject :only (wgs84-index
                                dimensions-at-res
                                modis-sample)]
        [forma.source.modis :only (modis->latlon latlon->modis pixels-at-res)]
        [clojure.string :only (split)]
        [clojure.contrib.duck-streams :only (read-lines with-out-writer)])
  (:require [cascalog.ops :as c]))

;; Worth keeping

(defn index-textfile
  "Prepend a row index to a text file `old-file-name`, located at `base-path`,
  and save in the same directory with new name `new-file-name`. The `num-drop`
  parameter specifies the number of lines to drop from the beginning of the 
  text file.  Note that indexing begins with the first textline not dropped,
  and starts at 0. This function was originally written to clean an ASCII
  raster file for input into the sampling functions."
  [base-path old-file-name new-file-name num-drop]
  (let [old-file (str base-path old-file-name)
        new-file (str base-path new-file-name)]
    (with-out-writer new-file
      (doseq [line (drop num-drop
                         (map-indexed
                          (fn [idx line] (str (- idx num-drop) " " line))
                          (read-lines old-file)))]
        (println line)))))



(def man-str "try1,try2,try3,try4,try5")
(defn mangle
  [line]
  (split line #","))

(defmapop add-mangled
  [x]
  (mangle man-str))

(defmapcatop add-mangled-catop
  [x]
  (mangle man-str))

(defmapop add-2-fields
  [x]
  [1 4])

(def source
  (memory-source-tap [["a" 1] 
                      ["b" 2] 
                      ["a" 3]]))

(defn queer-test [tuple-source]
  (<- [?c ?x ?lat ?lon ?d ?fire]
      (tuple-source _ ?c)
      (add-mangled ?c :> ?x ?lat ?lon ?d ?fire)))

(defn queer-catop [tuple-source]
  (let [sub-source (queer-test tuple-source)]
    (<- [?c ?str]
        (sub-source ?c ?x ?lat ?lon ?d ?fire)
        (add-mangled-catop ?c :> ?str))))

(comment
  (?- (stdout)
      (queer-catop source)))

#_(defn run-job [path]
  (let [source (hfs-textline path)]
    (?<- (stdout)
         [?mod-h ?mod-v ?sample ?line ?fire-count]
         (source ?line)
         (mangle ?line :> _ ?lat ?lon ?fire)
         (< ?fire 500)
         (latlon->modis ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
         (c/count ?fire-count))))


(defn to-nums
  [& strings]
  (map #(Float. %) strings))

(defn mangle
  [line]
  (apply to-nums
         (subvec (split line #",") 0 3)))

(def fire-file "/Users/danhammer/Desktop/workspace/data/fires/MCD14DL.2011075.txt")

(defmapop latlon [lat lon]
  [(latlon->modis "1000" lat lon)])

(defn mangle [line]
  (map (fn [val]
         (try (Float. val)
              (catch Exception _ val)))
       (split line #",")))

(defn fire-count [path]
  (let [source (hfs-textline path)]
    (?<- (stdout)
         [?lat ?lon ?mod-h ?mod-v ?sample ?line ?temp]
         (source ?line)
         (mangle ?line :> ?lat ?lon ?temp _ _ _ _ _ _ _ _ _)
         (> ?temp 330)
         (latlon->modis "1000" ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
         ;; (c/count ?fire-count)
         )))

(defn fire-source [source]
  (<- [?lat ?lon ?temp]
      (source ?line)
      (mangle ?line :> ?lat ?lon ?temp _ _ _ _ _ _ _ _ _)))

(defn fire-count [path]
  (let [f-source (-> path hfs-textline fire-source)]
    (<- [?mod-h ?mod-v ?sample ?line ?fire-count]
        ;; [?fire-count]
        (f-source ?lat ?lon ?temp)
        (> ?temp 330)
        (latlon->modis "1000" ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (c/count ?fire-count))))

(defn count-the-count [path-source]
  (let [sub-source (fire-count path-source)]
    (<- [?f-count ?c-count]
        (sub-source _ _ _ _ ?f-count)
        (c/count ?c-count))))

(comment
  (?- (stdout)
      (count-the-count fire-file))
  (?- (stdout)
      (fire-count fire-file))
  )

(defn queer-test [tuple-source]
  (<- [?c ?x ?lat ?lon ?d ?fire]
      (tuple-source _ ?c)
      (add-mangled ?c :> ?x ?lat ?lon ?d ?fire)))

(defn queer-catop [tuple-source]
  (let [sub-source (queer-test tuple-source)]
    (<- [?c ?x ?lat ?str]
        (sub-source ?c ?x ?lat ?lon ?d ?fire)
        (add-mangled-catop ?c :> ?str))))

;; (def cntry-file "/Users/danhammer/Desktop/grid.txt")

;; (defn cntry-get-id [line]
;;   (first (map (fn [val]
;;                 (try (Float. val)
;;                      (catch Exception _ val)))
;;               (split line #" "))))

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

;; (defn get-unique-cntry
;;   [int-array]
;;   (first (filter pos? int-array)))

(defmapcatop free-cols
  [row]
  (map-indexed vector row))

(defn ascii-source [path]
  (let [source (hfs-textline path)]
    (<- [?row ?col ?val]
        (source ?line)
        (liberate ?line :> ?row ?row-vec)
        (free-cols ?row-vec :> ?col ?val))))

;; swap this source out for one that contains mod-h, mod-y, line, and
;; sample, which should be generated dynamically, I think

(def temp-mod-source
  (memory-source-tap [["1000" 7 28 208 2]
                      ["1000" 7 28 208 3]
                      ["1000" 7 28 208 4]
                      ["1000" 7 28 208 5]
                      ["1000" 7 28 208 6]]))

;; can you have more than once source in a cascalog query?

;; now, we need to alculate the row and column of an ascii grid that a
;; particular pixel falls in, given the ascii characteristics and a
;; source of pixel characteristics (mod-h, mod-v, line, sample, lat,
;; and lon).

(def ascii-path "/Users/danhammer/Desktop/grid.txt")
(def ascii-info {:ncols 3600 :nrows 1737 :xulcorner -180 :yulcorner 83.7 :cellsize 0.1 :nodata -9999})

(defn sample-modis
  [mod-source ascii-path ascii-info]
  (let [grid-source (ascii-source ascii-path)]
    (?<- (stdout)
         [?line ?sample ?row ?col ?val]
         (grid-source ?row ?col ?val)
         (mod-source ?res ?mod-v ?mod-h ?line ?sample)
         (modis-sample ascii-info ?res ?mod-h ?mod-v ?sample ?line :> ?row ?col))))

(def source
  (memory-source-tap [["a" 1] 
                      ["b" 2] 
                      ["a" 3]]))

(defn queer-test [tuple-source]
  (<- [?c ?x ?lat ?lon ?d ?fire]
      (tuple-source _ ?c)
      (add-mangled ?c :> ?x ?lat ?lon ?d ?fire)))

(defn queer-catop [tuple-source]
  (let [sub-source (queer-test tuple-source)]
    (<- [?c ?str]
        (sub-source ?c ?x ?lat ?lon ?d ?fire)
        (add-mangled-catop ?c :> ?str))))

(defn mod-test
  [source]
  (?<- (stdout)
       [?mod-v ?mod-h ?line ?sample]
       (source ?mod-v ?mod-h ?line ?sample)))


(defn run-sample-test
  [path-to-text-source]
  (?- (stdout)
      (ascii-source path-to-text-source)))


