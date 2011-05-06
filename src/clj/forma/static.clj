(ns forma.static)

;; ### FORMA Constants
;;
;; Aside from our EC2 information, this is the only data we need to
;; supply for the first phase of operations. Everything else should be
;; able to remain local to individual modules.

(def forma-subsets
  [:ndvi :evi :qual :reli])

;; Arbitrary number of pixels slurped at a time off of a MODIS raster
;; band. For 1km data, each MODIS tile is 1200x1200 pixels; dealing
;; with each pixel individually would incur unacceptable IO costs
;; within hadoop. We currently fix the chunk size at 24,000, resulting
;; in 60 chunks per 1km data. Sharper resolution -> more chunks!

(def chunk-size 24000)

;; TODO: Comments.
(def static-datasets
  {:gadm {:ncols 36001
          :nrows 13962
          :xulcorner -180.000001
          :yulcorner 83.635972
          :cellsize 0.01
          :nodata -9999}
   :ecoid {:ncols 36000
           :nrows 17352
           :xulcorner -179.99996728576
           :yulcorner 83.628027
           :cellsize 0.01
           :nodata -9999}
   :hansen {:ncols 86223
            :nrows 19240
            :xulcorner -179.99998844516
            :yulcorner 40.164567
            :cellsize 0.0041752289295106
            :nodata -9999}
   :vcf {:ncols 86223
         :nrows 19240
         :xulcorner -179.99998844516
         :yulcorner 40.164567
         :cellsize 0.0041752289295106
         :nodata -9999}})
