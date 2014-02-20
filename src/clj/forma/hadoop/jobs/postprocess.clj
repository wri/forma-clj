(ns forma.hadoop.jobs.postprocess
  "Functions and Cascalog queries for postprocessing FORMA data for
   common data model et al."
  (:use [cascalog.api]
        [forma.source.gadmiso :only (gadm->iso gadm2->iso)]
        [forma.gfw.cdm :only (latlon->tile, latlon-valid?, meters->maptile)]
        [forma.utils :only (positions)])
  (:require [forma.postprocess.output :as o]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [cascalog.ops :as c]))

(def DEFAULT-DISCOUNT 1)

(defn split-line
  "Split a line of text using the supplied regular expression"
  [line re]
  (clojure.string/split line re))

(defbufferop min-period
  "Returns the minimum value in tuples."
  [tuples]
  [(reduce min (map first tuples))])

(defn first-hit
  "Returns the index of the first value in a vector of numbers that is
  greater than or equal to a threshold. `thresh` - The threshold
  value.  `series` - A vector of numbers.

  Example usage:
    (first-hit 5 [1 2 3 4 5 6 7 8 9 10]) => 4"
  [thresh series]
  (first (positions (partial <= thresh) series)))

(defn merge-gadm
  "Returns a source of probability series along with the appropriate gadm v.2 code."
  [forma-src gadm2-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?gadm2 ?start-idx ?prob-series]
      (forma-src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?prob-series)
      (gadm2-src ?s-res ?mod-h ?mod-v ?sample ?line ?gadm2)))

(defn hansen-latlon->cdm
  "Returns a Cascalog query that transforms Hansen latlon data into map tile
  coordinates. `src` - The source tap.  `zoom` - The map zoom level.
  `tres` - The temporal resolution."
  [src zoom tres]
  (let [epoch (date/datetime->period tres "2000-01-01")
        hansen (date/datetime->period tres "2010-12-31")
        period (- hansen epoch)]
    (<- [?x ?y ?z ?p]
        (src ?line)
        (split-line ?line #"," :> ?lon-str ?lat-str _)
        ((c/each #'read-string) ?lat-str ?lon-str :> ?lat ?lon)
        (latlon-valid? ?lat ?lon) ;; Skip if lat/lon invalid.
        (identity period :> ?p)
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))

(defn hansen-xy->cdm
  "Returns a Cascalog query that transforms Hansen xy data into map tile
  coordinates. `src` - The source tap.  `zoom` - The map zoom level.
  `tres` - The temporal resolution."
  [src zoom tres]
  (let [epoch (date/datetime->period tres "2000-01-01")
        hansen (date/datetime->period tres "2010-12-31")
        period (- hansen epoch)]
    (<- [?x ?y ?z ?p]
        (src ?line)
        (split-line ?line #"," :> ?xm-str ?ym-str _)
        ((c/each #'read-string) ?xm-str ?ym-str :> ?xm ?ym)
        (identity period :> ?p)
        (meters->maptile ?xm ?ym zoom :> ?x ?y ?z))))


(defn discount
  [disc coll]
  [(vec (map (partial * disc) coll))])

(defn forma->cdm
  "Returns a Cascalog generator that transforms FORMA data into map
    tile coordinates.  `start` - Estimation start period date string.
    `src` - The source tap for FORMA data.  `gadm-src` - a sequence
    file source with mod-h, mod-v, sample, line, and gadm. `thresh` -
    The threshold number for valid detections (0-100, integer).
    `tres` - The input temporal resolution (string).  `tres-out` - The
    output temporal resolution (string).  `zoom` - The map zoom
    level (integer). `min-period` ensures there are no duplicate records
    due to resampling pixels to CDM.

   Note that there may be a handful of pixels with `NA` values in the
   probability series that could cause this job to fail. The `clean-probs`
   now checks for this and replaces `NA` with the `nodata` value.


 Example usage:
    (forma->cdm (hfs-seqfile \"s3n://pailbucket/output/run-2013-05-09/merged-estimated\")
                -9999.0
                17
                \"16\"
                \"32\"
                \"2005-12-31\"
                50)"
  [src nodata zoom tres tres-out start thresh & [disc-map]]
  (let [epoch (date/datetime->period tres-out "2000-01-01")
        disc-map (or disc-map {})]
    (<- [?x ?y ?z ?p]
        (src ?sres ?modh ?modv ?s ?l ?start-period ?prob-series ?gadm2 ?ecoid)
        (get disc-map ?ecoid DEFAULT-DISCOUNT :> ?disc)
        (discount ?disc ?prob-series :> ?series)
        (o/clean-probs ?series nodata :> ?clean-series)
        (gadm2->iso ?gadm2 :> ?iso)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ ?start-period ?first-hit-idx :> ?period)
        (date/shift-resolution tres tres-out ?period :> ?period-new-res)
        (- ?period-new-res epoch :> ?rp)
        (min-period ?rp :> ?p)
        (date/period->datetime tres-out ?period-new-res :> ?date-str)
        (r/modis->latlon ?sres ?modh ?modv ?s ?l :> ?lat ?lon)
        (latlon-valid? ?lat ?lon)
        (latlon->tile ?lat ?lon zoom :> ?x ?y ?z))))

(defn spark-hits
  "Prep for generate counts by country, for spark graphs on GFW site.

   Example usage:
    (spark-hits (hfs-seqfile \"/home/dan/Dropbox/local/output\")
                (hfs-seqfile \"/tmp/forma/data/gadm-path\")
                -9999.0
                17
                \"16\"
                \"32\"
                \"2005-12-31\"
                50)"
  [src nodata tres tres-out start thresh]
  (let [epoch (date/datetime->period tres-out "2000-01-01")]
    (<- [?iso ?sres ?mod-h ?mod-v ?s ?l ?p]
        (src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?prob-series ?gadm2)
        (gadm->iso ?gadm :> ?iso)
        (o/clean-probs ?prob-series nodata :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ ?start ?first-hit-idx :> ?period)
        (date/shift-resolution tres tres-out ?period :> ?period-new-res)
        (- ?period-new-res epoch :> ?rp)
        (min-period ?rp :> ?p)
        (:trap (hfs-seqfile "s3n://formatemp/output/spark-trapped")))))

(defn spark-graphify
  [src nodata tres tres-out start thresh]
  (let [spark-src (spark-hits src nodata tres tres-out start thresh)]
    (<- [?iso ?p ?ct]
        (spark-src ?iso _ _ _ _ _ ?p)
        (c/count ?ct))))

(defn probs->country-stats
  "Given a source of probabilty series pre-joined with GADM v.2, count
  up the number of hits per country in preparation for upload to
  CartoDB country stats table."
  [thresh nodata t-res t-res-out probs-gadm-src]
  (let [epoch (date/datetime->period t-res-out "2000-01-01")]
    (<- [?iso ?cdm-period ?date-str ?count] ;; ?period ?count
        (probs-gadm-src ?s-res ?mod-h ?mod-v ?s ?l ?start-idx ?prob-series ?gadm2)
        (gadm2->iso ?gadm2 :> ?iso)
        (o/clean-probs ?prob-series nodata :> ?clean-series)
        (first-hit thresh ?clean-series :> ?first-hit-idx)
        (+ ?start-idx ?first-hit-idx :> ?period)
        (date/shift-resolution t-res t-res-out ?period :> ?period-new-res)
        (- ?period-new-res epoch :> ?cdm-period)
        (date/period->datetime t-res-out ?period-new-res :> ?date-str)
        (c/count ?count))))

(defn forma->blue-raster
  "Prepare data for use by Blue Raster. Expects `src` to include GADM2
  and ecoregion fields already."
  [src static-src nodata & [disc-map]]
  (let [disc-map (or disc-map {})]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?lat ?lon ?iso ?vcf ?gadm2
         ?ecoid ?hansen ?clean-series]
        (src ?s-res ?mod-h ?mod-v ?sample ?line ?start ?prob-series ?gadm2 ?ecoid)
        (static-src ?s-res ?mod-h ?mod-v ?sample ?line ?vcf _ _ ?hansen _)
        (get disc-map ?ecoid DEFAULT-DISCOUNT :> ?disc)
        (discount ?disc ?prob-series :> ?series)
        (o/clean-probs ?series nodata :> ?clean-series)
        (gadm2->iso ?gadm2 :> ?iso)
        (r/modis->latlon ?s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon))))

(defn forma-download
  "Prepare data for bulk download from S3. Expects `src` to include
  GADM2 and ecoregion fields already. Count operation forces reduce
  step, which avoids producing the thousands of output files that
  would otherwise be created by all the mappers."
  [src thresh t-res nodata & [disc-map]]
  (let [format-str "%.8f"
        disc-map (or disc-map {})
        query (<- [?lat-str ?lon-str ?iso ?gadm2 ?date-str ?count]
                  (src ?s-res ?mod-h ?mod-v ?sample ?line ?start-final
                       ?prob-series ?gadm2 ?ecoid)
                  (get disc-map ?ecoid DEFAULT-DISCOUNT :> ?disc)
                  (discount ?disc ?prob-series :> ?series)
                  (o/clean-probs ?series nodata :> ?clean-series)
                  (gadm2->iso ?gadm2 :> ?iso)
                  (r/modis->latlon ?s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
                  (format format-str ?lat :> ?lat-str)
                  (format format-str ?lon :> ?lon-str)
                  (first-hit thresh ?clean-series :> ?first-hit-idx)
                  (+ ?start-final ?first-hit-idx :> ?period)
                  (date/period->datetime t-res ?period :> ?date-str)
                  (c/count ?count))]
    (<- [?lat-str ?lon-str ?iso ?gadm2 ?date-str]
        (query ?lat-str ?lon-str ?iso ?gadm2 ?date-str _))))
