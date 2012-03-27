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

;; TODO: Move this business down into a configuration file.
(def static-datasets
  "These are the static datasets, described briefly
  below. Each dataset must be formatted as:

    {:corner [lon, lat]
     :travel [lon-dir, lat-dir]
     :step step-size
     :nodata no-data-value}

   [NOAA PRECL](http://goo.gl/yDQhA) :: Global precipitation rates in
   mm/day, by month, gridded at 0.5 degree resolution. The linked dataset 
   is 2.5 degree -- we use a higher resolution dataset in binary 
   format. Considered static relative to frequency of MODIS data.

   gadm :: administrative boundaries, found
   [here](http://goo.gl/2N5CT), and converted to a raster with the
   cell value given by ID_2, an integer value corresponding to the
   most detailed administrative unit. .01 degree resolution chosen 
   for simplicity, but should have limited impact on where a pixel
   falls aside from at borders.

   ecoid :: ecoregions according to the World Wildlife Fund. The
   technical paper can be found [here](http://goo.gl/yvssq). The
   polygons are converted to a raster for sampling - much like the
   admin boundaries - where each cell is assigned the value of the
   ecoregion id, the most detailed eco unit. .01 degree resolution
   chosen for simplicity, but should have limited impact on where 
   a pixel falls aside from at borders.

   hansen :: forest cover loss hotspots data for training at 500m
   resolution, found [here](http://goo.gl/HqvCW). Note that this has
   been projected into WGS84 from Sinusoidal (463.3127m res).  The
   time period is 2000-2005. UPDATE: This may be superceded by the 
   use of an unreprojected ascii grid that lines up perfectly with
   MODIS data and can therefore be assigned to MODIS pixels with
   no error from reprojection.

   vcf :: vegetation continuous field index, derived from MODIS
   products for the year 2000, found [here](http://goo.gl/KW8y1), and
   used to define the extent of the sample area, given by the Forest
   Cover Loss Hotspots training data set. UPDATE: This may be 
   superceded by the use of an unreprojected ascii grid that lines up 
   perfectly with MODIS data and can therefore be assigned to MODIS 
   pixels with no error from reprojection.

   border :: index from 1-20 representing 500m increments of distance
   to a coastline as defined by the GADM Level 0 dataset available at 
   GADM.org. Coastlines are defined as the border between land and large
   bodies of water (water is 0, undefined -9999). Land is defined by the 
   polygon of country boundaries. Projected World Sinusoidal for distance 
   calculations with ~463m resolution. Then reprojected WGS84 with 
   .0083333 resolution for conversion to ascii grid, with bilinear 
   interpolation of pixel values."
  {:precl  {:corner [0 -90]
            :travel [+ +]
            :step 0.5
            :nodata -999}
   :gadm   {:corner [-180.000001 83.635972]
            :travel [+ -]
            :step 0.01
            :nodata -9999}
   :ecoid  {:corner [-179.99996728576 83.628027]
            :travel [+ -]
            :step 0.01
            :nodata -9999}
   :hansen {:corner [-179.99998844516 40.164567]
            :travel [+ -]
            :step 0.0041752289295106
            :nodata -9999}
   :vcf    {:corner [-179.99998844516 40.164567]
            :travel [+ -]
            :step 0.0041752289295106
            :nodata -9999}
   :border {:corner [-179.999988445 40.1690038607]
            :travel [+ -]
            :step 0.0041666667
            :nodata -9999}})
