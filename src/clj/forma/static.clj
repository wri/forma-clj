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

(def static-datasets
  ^{:doc "These are the static datasets, described briefly below.  The inner
   keys (e.g., :ncols) are a quick transformation on the ASCII header, where
   the coordinates for the upper left corner are presented, rather than the
   lower left corner.  The only value that changes, then, is the y-coord.

   gadm   :: administrative boundaries, found [here](http://goo.gl/2N5CT),
   and converted to a raster with the cell value given by ID_2, an integer
   value corresponding to the most detailed administrative unit.

   ecoid  :: ecoregions according to the World Wildlife Fund. The technical
   paper can be found [here](http://goo.gl/yvssq). The polygons are converted
   to a raster for sampling - much like the admin boundaries - where each cell
   is assigned the value of the ecoregion id, the most detailed eco unit.

   hansen :: forest cover loss hotspots data for training at 500m resolution,
   found [here](http://goo.gl/HqvCW). Note that this has been projected into
   WGS84 from Sinusoidal (463.3127m res).  The time period is 2000-2005.

   vcf    :: vegetation continuous field index, derived from MODIS products for
   the year 2000, found [here](http://goo.gl/KW8y1), and used to define the
   extent of the sample area, given by the Forest Cover Loss Hotspots training
   data set."}
  
  {:gadm   {:ncols 36001
            :nrows 13962
            :xulcorner -180.000001
            :yulcorner 83.635972
            :cellsize 0.01
            :nodata -9999}
   :ecoid  {:ncols 36000
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
   :vcf    {:ncols 86223
            :nrows 19240
            :xulcorner -179.99998844516
            :yulcorner 40.164567
            :cellsize 0.0041752289295106
            :nodata -9999}})
