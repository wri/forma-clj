(ns forma.source.static
  (:use cascalog.api
        [clojure.string :only (split)]
        [forma.source.modis :only (latlon->modis)])
  (:require [cascalog.ops :as c]))

(def man-str "adsfkj,adsklf,dafj,adka,dafj")
(defn mangle
  [line]
  (split line #","))

(defmapop add-mangled
  [x]
  (mangle "adsfkj,adsklf,dafj,adka,dafj"))

(defmapcatop add-mangled-catop
  [x]
  (mangle "adsfkj,adsklf,dafj,adka,dafj"))

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
    (<- [?c ?x ?lat ?str]
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

(def cntry-file "/Users/danhammer/Desktop/grid.txt")

(defn cntry-get-id [line]
  (first (map (fn [val]
                (try (Float. val)
                     (catch Exception _ val)))
              (split line #" "))))

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

(defn get-unique-cntry
  [int-array]
  (first (filter pos? int-array)))

(defmapcatop free-cols
  [row]
  (map-indexed vector row))

(defn cntry-source [path]
  (let [source (hfs-textline path)]
    (<- [?row-idx ?col-idx ?val]
        (source ?line)
        (liberate ?line :> ?row-idx ?row-vec)
        (free-cols ?row-vec :> ?col-idx ?val))))

(comment 
  (?- (stdout)
      (cntry-source cntry-file)))


