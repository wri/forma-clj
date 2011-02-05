(ns forma.hdf
  (:use forma.hadoop
        (cascalog [api :only (defmapop defmapcatop)]
                  [io :only (temp-dir)])
        (clojure.contrib [io :only (file copy delete-file-recursively)]
                         [string :only (substring? as-str drop)]
                         [seq-utils :only (find-first)]))
  (:import [java.util Hashtable]
           [java.io File]
           [org.gdal.gdal gdal Dataset Band]
           [org.gdal.gdalconst gdalconstConstants]
           [org.apache.hadoop.io BytesWritable]))

(set! *warn-on-reflection* true)

;; ##Constants

(def
  #^{:doc "Map between symbols and chosen substrings of
MODIS subdataset keys."}
  modis-subsets
  {:evi "monthly EVI"
   :qual "VI Quality"
   :red "red reflectance"
   :nir "NIR reflectance"
   :azi "azimuth"
   :blue "blue reflectance"
   :reli "monthly pixel"
   :viewz "view zenith"
   :ndvi "NDVI"
   :sunz "sun zenith"})

(def
  #^{:doc "Arbitrary number of pixels in a chunk of MODIS data."}
  chunk-size
  24000)

;; ## MODIS Unpacking

(defn metadata
  "Returns the metadata hashtable for the supplied (opened) MODIS file."
  ([^Dataset modis key]
     (.GetMetadata_Dict modis key))
  ([modis]
     (metadata modis "")))

;; TODO -- describe why we only take the names business. It's because we're
;; skipping the descriptions.
;; The MODIS SUBDATASET dictionary holds entries of the form:
;; Key: SUBDATASET_2_NAME
;; Val: HDF4_EOS:EOS_GRID:"/path/to/modis.hdf":MOD_Grid_monthly_1km_VI:1 km
;; monthly EVI
;; Key: SUBDATASET_4_DESC

(defn subdataset-names
  "Returns the NAME entries of the SUBDATASETS metadata Hashtable for
  a given filepath."
  [hdf-file]
  (let [path (str hdf-file)
        dataset (gdal/Open path)]
    (try
      (vals (filter #(substring? "_NAME" (key %))
                    (metadata dataset "SUBDATASETS")))
      (finally (.delete dataset)))))


;; TODO --update docs, since we now don't unpack the entry.
(defn subdataset-key
  "Takes a long-form path to a MODIS subdataset, and checks
  to see if any of the values of the modis-subsets map can be found as
  substrings. If we find one, we return the associated key, cast to a
  String."
  [path]
  (as-str (find-first #(substring? (% modis-subsets) path)
                      (keys modis-subsets))))

;;  TODO-- check docs, since we now don't check against _NAME
(defn dataset-filter
  "Generates a predicate function that checks the a subdataset name
   from the SUBDATASETS metadata dictionary against a supplied set of
   acceptable datasets."
  [good-keys]
  (let [substrings (map modis-subsets good-keys)]
    (fn [name]
      (some #(substring? % name) substrings))))

;; TODO --update docs, since we're not accepting an entry anymore
(defn make-subdataset
  "Accepts an entry from the SUBDATASETS Hashtable of a MODIS
 Dataset (remember, this is just a path to the subdataset), and
 returns a 2-tuple - (modis-subsets key, Dataset)."
  [path]
  (vector (subdataset-key path) (gdal/Open path)))

;; ##Cascalog Custom Functions

(defmapcatop
  #^{:doc "Stateful approach to unpacking HDF files. Registers all
gdal formats, Creates a temp directory, then saves the byte array to
disk. This byte array is processed with gdal. On teardown, the temp
directory is destroyed. Function returns the decompressed MODIS file
as a 1-tuple."}
  [unpack [to-keep]] {:stateful true}
  ([]
     (do
       (gdal/AllRegister)
       (temp-dir "hdf")))
  ([tdir stream]
     (let [bytes (get-bytes stream)
           temp-hdf (file tdir (hash-str stream))
           keep? (dataset-filter to-keep)]
       (do
         (copy bytes temp-hdf)
         (map make-subdataset
              (filter keep? (subdataset-names temp-hdf))))))
  ([tdir]
     (delete-file-recursively tdir)))

;; ## Dataset Chunking
;; This needs some more documentation, but these are the functions
;; that deal with opened datasets.

(defmapop
  #^{:doc "Generates metadata values for a given unpacked MODIS
Dataset and a seq of keys."}
  [meta-values [meta-keys]] [modis]
  (let [^Hashtable metadict (metadata modis)]
    (map #(.get metadict %) meta-keys)))

(defn raster-array
  "Unpacks the data inside of a MODIS band into a 1xN integer array."
  [^Dataset data]
  (let [^Band band (.GetRasterBand data 1)
        type (gdalconstConstants/GDT_UInt16)
        width (.GetXSize band)
        height (.GetYSize band)
        ret (int-array (* width height))]
    (do (.ReadRaster band 0 0 width height type ret) ret)))

(defn tile-position
  "For a given MODIS chunk and index within that chunk, returns [line,
  sample] within the MODIS tile."
  [chunk index]
  (let [line (* chunk chunk-size)
        sample (+ line index)]
    (vector line sample)))

(defn parse-ints [coll]
  (map #(Integer/parseInt %) coll))

;; This is the final version of the function below -- I've included a
;; few other ways of accomplishing the same thing, for demonstration
;; purposes. Robin, Dan... enjoy!

;; TODO -- modify this function to actually return the tile
;; resolution, along with the tiling scheme. As described at this
;; site:
;; http://modis-250m.nascom.nasa.gov/developers/tileid.html
;; The first digit is used to identify the projection, while the
;; second digit is used to specify the tile size. 1 is 1km data (full
;; tile size) , 2 is quarter size (500m data), and 4 is 16th tile
;; size, or 250m data.
(defn split-id
  "Splits a TileID metadata string of the form \"DDXXXYYY\", where DD
can be tossed, into integer representations of XXX and YYY. These
represent MODIS tile coordinates."
  [tileid]
    (parse-ints
   (map (partial apply str)
        (->> tileid (drop 2) (partition 3)))))

;; TODO -- show Dan and Robin, and remove these demonstration functions.

;; Misleading! Here, we're actually calling (subs tileid...) in the
;; first elements of each of the other vectors, not on each vector in
;; turn. I won't go with something like this; it works, but it's confusing.
(fn [tileid]
  (parse-ints
   (map (partial subs tileid) [2 5] [5 8])))

;; This is a little bit better, but not really. Now we have a vector
;; of vectors -- partial takes the stuff after it, and builds a
;; function that takes less arguments than that stuff needs. (in this
;; case, we get: the function (apply subs tileid ...), where the
;; ellipsis can be any group of arguments. Since we're mapping that,
;; we get [2 5] and [5 8] fed in in turn.
(fn [tileid]
  (parse-ints
   (map (partial apply subs tileid) [[2 5] [5 8]])))

;; Another way to do the same thing. In this case, juxt gives us a
;; sequence that is the juxtaposition of all of the functions. When we
;; send in an argument, we get the juxtaposition of the results -- in
;; this case, ((subs tileid 2 5) (subs tileid 5 8)), where subs is
;; substring (as above).
(fn [tileid]
  (parse-ints
   ((juxt #(subs % 2 5) #(subs % 5 8)) tileid)))

;; This is the same as the final; it actually compiles to the same
;; byte code, I'm just not using the threading macro here. I don't
;; know if the threading macro makes the code more clear, in this
;; case, but it's a good example of little tweaks for readability,
;; independent of performance.
(fn [tileid]
  (parse-ints
   (map (partial apply str)
        (partition 3 (drop 2 tileid)))))