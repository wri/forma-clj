(ns forma.source.hdf-test
  (:use [forma.source.hdf] :reload)
  (:use cascalog.api
        forma.testing
        midje.sweet)
  (:require [forma.hadoop.io :as io]
            [cascalog.ops :as c]))

(def hdf-path
  (dev-path "/testdata/MOD13A3/MOD13A3.A2000032.h03v11.005.2006271174459.hdf"))

(fact "metadata test."
  (let [meta (with-gdal-open [dataset hdf-path]
               (metadata dataset))]
    meta => map?
    meta => (contains {"ASSOCIATEDPLATFORMSHORTNAME" "Terra"})))


(fact "modis-subsets test. All keys in `modis-subsets` should be found
inside the HDF file located at `hdf-path`."
  (map subdataset-key
       (subdataset-names hdf-path)) => (just (keys modis-subsets) :in-any-order))

(facts "filter checker."
  (let [test-keys (fn [keys]
                    (filter (dataset-filter keys)
                            (subdataset-names hdf-path)))]
    "If the user supplies a sequence of appropriate keys, the function
will return a sequence of subdataset names corresponding to the
supplied keys (in order)."
    (test-keys #{:ndvi :evi}) => #(= (count %) 2)
    (test-keys [:ndvi :evi])  => #(= (count %) 2)

    "If a key is missing, the generated function throws an assertion
error."
    (test-keys #{:ndv}) => (throws AssertionError)))

(tabular
 (fact "Test that our cascalog queries are returning the proper
 subdatasets."
   (let [src (io/hfs-wholefile hdf-path)
         [datasets n] ((juxt identity count) ?dataset-seq)]
     (fact?<- ?result [?count]
              (src ?filename ?hdf)
              (unpack-modis [datasets] ?hdf :> ?dataset ?freetile)
              (c/count ?count))))
 ?dataset-seq      ?result
 [:ndvi :evi]      [[2]]
 [:evi :reli :mir] [[3]])

(tabular
 (fact "Test ensuring that raster-chunks can be serialized, and that
 they produce the proper number of chunks for the supplied
 chunk-size. (By pushing raster-chunks down into a subquery, we force
 serialization of tuples before they're utilized inside of the final
 `fact?<-` query.)"
   (let [src (io/hfs-wholefile hdf-path)
         chunk-size ?c-size
         subquery (<- [?dataset ?chunkid ?chunk]
                      (src ?filename ?hdf)
                      (unpack-modis [[:ndvi]] ?hdf :> ?dataset ?freetile)
                      (raster-chunks [chunk-size] ?freetile :> ?chunkid ?chunk))]
     (fact?<- [[?num-chunks]] [?count]
              (subquery ?dataset ?chunkid ?chunk)
              (c/count ?count))))
 ?c-size ?num-chunks
 24000   60
 48000   30
 96000   15)

(tabular
 (fact "Check on the proper unpacking of the HDF file metadata map."
   (let [src (io/hfs-wholefile hdf-path)
         [ks xs] ((juxt keys vals) ?meta-map)]
     (fact?<- [xs]
              [?productname ?tileid ?date]
              (src ?filename ?hdf)
              (unpack-modis [[:ndvi]] ?hdf :> ?dataset ?freetile)
              (meta-values [ks] ?freetile :> ?productname ?tileid ?date))))
 ?meta-map
 {"RANGEBEGINNINGDATE" "2000-02-01"
  "SHORTNAME"          "MOD13A3"
  "TileID"             "51003011"})

(fact "tileid->res tests."
  (tileid->res "51003011") => "1000"
  (tileid->res "52003011") => "500"
  (tileid->res "54003011") => "250")

(fact "split-id tests."
  (split-id "51003011") => ["1000" "003011"]
  (split-id "52010010") => ["500" "010010"]
  (split-id "54028027") => ["250" "028027"])

(fact "t-res cascalog shell test. (Really, this could go in
forma.source.modis, with `temporal-res`.)"
  (t-res "MOD13A3") => "32")

(fact "modis-chunks test. `modis-chunks` is bound to a subquery, which
is used as a source for the final count aggregator. We check that the
chunk-size makes sense for the supplied dataset.

Note that this only works when the file at hdf-path has a spatial
resolution of 1000m; otherwise, the relationship between chunk-size
and total chunks becomes off."
  (let [subquery (->> (io/hfs-wholefile hdf-path)
                      (modis-chunks [:ndvi] 24000))]
    (fact?<- [[60]] [?count]
             (subquery ?dataset ?s-res ?t-res ?tstring ?date ?chunkid ?chunk)
             (c/count ?count))))
