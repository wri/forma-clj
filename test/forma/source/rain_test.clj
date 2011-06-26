(ns forma.source.rain-test
  (:use [forma.source.rain] :reload)
  (:use midje.sweet
        [forma.static :only (static-datasets)])
  (:require [forma.testing :as t])
  (:import  [java.io InputStream]
            [java.util.zip GZIPInputStream]))

(def precl-path
  (t/dev-path "/testdata/PRECL/precl_mon_v1.0.lnx.2000.gri0.5m.gz"))

(fact float-bytes => 4)

(fact (floats-for-step 0.5) => 1036800)

(??<- [?count]
      )

(fact "modis-chunks test. `modis-chunks` is bound to a subquery, which
is used as a source for the final count aggregator. We check that the
chunk-size makes sense for the supplied dataset.

Note that this only works when the file at hdf-path has a spatial
resolution of 1000m; otherwise, the relationship between chunk-size
and total chunks becomes off."
  (let [ascii-map (:precl static-datasets)
        file-tap (io/hfs-wholefile precl-path)
        subquery (rain-chunks "1000" ascii-map 24000 file-tap pix-tap)
        [[chunk-count]] (??<- [?count]
                              (subquery ?dataset ?s-res ?t-res ?tstring ?date ?chunkid ?chunk)
                              (c/count ?count))]
    chunk-count => 60))
