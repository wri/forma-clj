(ns forma.hdf
  (:use (forma hadoop [conversion :only (to-period)])
        (cascalog api [io :only (temp-dir)])
        (clojure.contrib [seq-utils :only (find-first indexed)]))
  (:require (clojure.contrib [string :as s]
                             [io :as io]))
  (:import [java.util Hashtable]
           [java.io File]
           [org.gdal.gdal gdal Dataset Band]
           [org.gdal.gdalconst gdalconstConstants]
           [org.apache.hadoop.io BytesWritable]))

(set! *warn-on-reflection* true)

;; ## Constants

(def modis-subsets
  ^{:doc "Map between MODIS dataset identifiers (arbitrarily chosen
by the REDD team) and corresponding unique substrings, culled from
values in the SUBDATASETS hashtable of MOD13A3 HDF file. No simple,
descriptive tag exists within the metadata, as of 2/05/2011, so this
ends up being the best solution."}
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

;; ## MODIS Unpacking

(defn metadata
  "Returns the metadata hashtable for the supplied MODIS Dataset."
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
      (vals (filter #(s/substring? "_NAME" (key %))
                    (metadata dataset "SUBDATASETS")))
      (finally (.delete dataset)))))


;; TODO --update docs, since we now don't unpack the entry.
(defn subdataset-key
  "Takes a long-form path to a MODIS subdataset, and checks
  to see if any of the values of the modis-subsets map can be found as
  substrings. If we find one, we return the associated key, cast to a
  String."
  [path]
  (s/as-str (find-first #(s/substring? (% modis-subsets) path)
                        (keys modis-subsets))))

;;  TODO-- check docs, since we now don't check against _NAME
(defn dataset-filter
  "Generates a predicate function that checks the a subdataset name
   from the SUBDATASETS metadata dictionary against a supplied set of
   acceptable datasets."
  [good-keys]
  (let [substrings (map modis-subsets good-keys)]
    (fn [name]
      (some #(s/substring? % name) substrings))))

;; TODO --update docs, since we're not accepting an entry anymore
(defn make-subdataset
  "Accepts an entry from the SUBDATASETS Hashtable of a MODIS
 Dataset (remember, this is just a path to the subdataset), and
 returns a 2-tuple - (modis-subsets key, Dataset)."
  [path]
  (vector (subdataset-key path) (gdal/Open path)))

;; ##Cascalog Custom Functions

(defmapcatop [unpack-modis [to-keep]] {:stateful true}
  ^{:doc "Stateful approach to unpacking HDF files. Registers all
gdal formats, Creates a temp directory, then saves the byte array to
disk. This byte array is processed with gdal. On teardown, the temp
directory is destroyed. Function returns the decompressed MODIS file
as a 1-tuple."}
  ([]
     (do
       (gdal/AllRegister)
       (temp-dir "hdf")))
  ([tdir stream]
     (let [bytes (get-bytes stream)
           temp-hdf (io/file tdir (hash-str stream))
           keep? (dataset-filter to-keep)]
       (do
         (io/copy bytes temp-hdf)
         (map make-subdataset
              (filter keep? (subdataset-names temp-hdf))))))
  ([tdir]
     (io/delete-file-recursively tdir)))

;; ## Chunking Functions

;; TODO -- This needs some more documentation, but these are the
;; functions that open datasets, and chunk the resulting integer
;; arrays.

(defmapcatop [raster-chunks [chunk-size]]
  ^{:doc "Unpacks the data inside of a MODIS band into a lazy
sequence of chunks. See chunk-length for the default size."}
  [^Dataset data]
  (let [^Band band (.GetRasterBand data 1)
        width (.GetXSize band)
        height (.GetYSize band)
        ret (int-array (* width height))]
    (.ReadRaster band 0 0 width height ret)
    (indexed (partition chunk-size ret))))

;; ## Metadata Parsing

(defmapop [meta-values [meta-keys]]
  ^{:doc "Generates metadata values for a given unpacked MODIS
Dataset and a seq of keys."}
  [modis]
  (let [^Hashtable metadict (metadata modis)]
    (map #(.get metadict %) meta-keys)))

;; TODO -- add some good documentation here about what the next
;; section is accomplishing. I want to check marginalia to see the
;; best way to do this stuff.

;; It's described here:
;; http://modis-250m.nascom.nasa.gov/developers/tileid.html

;; The first digit is used to identify the projection, while the
;; second digit is used to specify the tile size. 1 is 1km data (full
;; tile size) , 2 is quarter size (500m data), and 4 is 16th tile
;; size, or 250m data.

(defn parse-ints
  "Converts all strings in the supplied collection to their integer
  representation."
  [coll]
  (map #(Integer/parseInt %) coll))

(def res-map
  ^{:doc "Map between the second digit in a MODIS TileID
metadata value and the corresponding resolution."}
  {:1 "1000"
   :2 "500"
   :4 "250"})

(defn tileid->res
  "Returns a string representation of the resolution referenced by the
supplied MODIS TileID."
  [tileid]
  (res-map (keyword (subs tileid 1 2))))

(defn tileid->xy
  "Extracts integer representations of the MODIS X and Y coordinates
referenced by the supplied MODIS TileID."
  [tileid]
  (parse-ints
   (map (partial apply str)
        (partition 3 (subs tileid 2)))))

(defn split-id
  "Returns a sequence containing the resolution, X and Y
  coordinates (on the MODIS grid) referenced by the supplied MODIS
  TileID."
  [tileid]
  (flatten
   ((juxt tileid->res
          tileid->xy) tileid)))

;; ## The Chunker!

(defn modis-chunks
  "Takes a source, either (hfs-wholefile dir) or (hfs-seqfile dir),
  and returns a number of 7-tuples that fully describe chunks of MODIS
  data for the supplied datasets. This function currently can't be
  refactored, as the gdal.Dataset object doesn't implement
  Serializable. This forces us to completely tease apart the HDF files
  at the source before we can return tuples from the subquery. Chunks
  are represented as seqs of floats. Be sure to convert to vector
  before running any sort of data analysis, as (seqs require linear
  time for lookups)."
  [source datasets chunk-size]
  (let [keys ["TileID" "PRODUCTIONDATETIME"]]
    (<- [?dataset ?res ?tile-x ?tile-y ?period ?chunkid ?chunk]
        (source ?filename ?hdf)
        (unpack-modis [datasets] ?hdf :> ?dataset ?freetile)
        (raster-chunks [chunk-size] ?freetile :> ?chunkid ?chunk)
        (meta-values [keys] ?freetile :> ?tileid ?juliantime)
        (split-id ?tileid :> ?res ?tile-x ?tile-y)        
        (to-period ?juliantime ?res :> ?period)
        (:distinct false))))


;; ## Fun Examples!

;; Guys, are a few ways to accomplish the task solved by tileid->xy,
;; shown above. I'm not sure I'm happy with the final version, so I've
;; left these here for us to peruse together. Enjoy!

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
;; we get [2 5] and [5 8] fed in in turn.)
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

;; This is the same as the next one; it actually compiles to the same
;; byte code, I'm just not using the threading macro here. I don't
;; know if the threading macro makes the code more clear, in this
;; case, but it's a good example of little tweaks for readability,
;; independent of performance.
(fn [tileid]
  (parse-ints
   (map (partial apply str)
        (partition 3 (s/drop 2 tileid)))))

;; The referenced one, with the threading macro. As you can see, it
;; takes each thing, evaluates it, and inserts it as the last thing in
;; the next entry. So, here we have tileid inserted, making (s/drop 2
;; tileid). The whole thing expands to the code above.
;; BE CAREFUL with this! It's a really good solution, but only when it
;; makes things clear -- not when it's used as a way to think
;; iteratively. There's usually some transformation on a list that you
;; can do to get the same effects as the threading macro. (Once we all
;; learn to code the Clojure Way, this stuff will be fine :)
(fn [tileid]
  (parse-ints
   (map (partial apply str)
        (->> tileid
             (s/drop 2)
             (partition 3)))))

;; I ended up going with this bad boy, because it looked closest to my
;; other function, tileid->res. Look at them together, to see how
;; they're sort of similar:
;;
;; (defn tileid->res
;;   "Returns a string representation of the resolution referenced by the
;; supplied MODIS TileID."
;;   [tileid]
;;   (res-map (keyword (subs tileid 1 2))))
;;
(fn [tileid]
  (parse-ints
   (map (partial apply str)
        (partition 3 (subs tileid 2)))))