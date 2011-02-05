(ns forma.hdf
  (:use forma.hadoop
        (cascalog [api :only (defmapop defmapcatop)]
                  [io :only (temp-dir)])
        (clojure.contrib [io :only (file copy delete-file-recursively)]
                         [string :only (substring? as-str)]
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

(defn split-id
  "Splits the TileID metadata string into integer x and y,
  representing MODIS tile coordinates."
  [tileid]
  (let [[_ _ tile-x tile-y] (re-find #"(\d{2})(\d{3})(\d{3})" tileid)]
    (map #(Integer/parseInt %) [tile-x tile-y])))

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