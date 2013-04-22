(ns forma.hadoop.jobs.api
  "We’ll generate an API master dataset after each update. This
  dataset will be vertically partitioned into sequence files using a
  template tap as follows:

  /api/{iso}/{year}"
  (:use cascalog.api
        [forma.source.gadmiso :only (gadm2->iso)])
  (:require [cascalog.ops :as c]
            [forma.postprocess.output :as o]
            [forma.reproject :as r]
            [forma.date-time :as date]
            [forma.utils :as u]
            [forma.hadoop.predicate :as p]))

(defmapcatop wide->long
  "Convert a series from wide to long form.

   Usage:
     (let [start-idx 827
           src [[1 [1 2 3]]]]
       (??<- [?id ?pd ?val]
             (src ?id ?series)
             (wide->long start-idx ?series :> ?pd ?val)))
     ;=> [[1 827 1] [1 828 2] [1 829 3]]"
  [start-idx series]
  (let [len (count series)
        idxs (range start-idx (inc (+ start-idx len)))]
    (map vector idxs series)))

(defn merge-gadm
  "Returns a source of probability series along with the appropriate gadm v.2 code."
  [forma-src gadm2-src]
  (<- [?s-res ?mod-h ?mod-v ?sample ?line ?gadm2 ?start-idx ?prob-series]
      (forma-src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?prob-series)
      (gadm2-src ?s-res ?mod-h ?mod-v ?sample ?line ?gadm2)))

(defn prob-series->tsv-api
  "Returns a Cascalog query that that converts output of
   `forma-estimate` to long form according to API spec. The merge with
   GADM data occurs prior to conversion to long form for API, as the
   conversion gives us tons of records and the join would take longer.

   ?year field is intended for use with template tap. ?iso-extra is
   included because the template tap removes :templatefields
   from :outfields, but we want the iso code included in the output."
  [est-map prob-src gadm2-src]
  (let [nodata (:nodata est-map)
        est-start (:est-start est-map)
        t-res (:t-res est-map)
        src (merge-gadm prob-src gadm2-src)]
    (<- [?lat ?lon ?iso ?iso-extra ?gadm2 ?date ?year ?prob]
        (src ?s-res ?mod-h ?mod-v ?sample ?line ?gadm2 ?start-idx ?prob-series)
        (gadm2->iso ?gadm2 :> ?iso)
        (p/add-fields ?iso :> ?iso-extra)
        (first ?prob-series :> ?first-elem)
        (symbol? ?first-elem :> false) ;; screen out NA
        (o/clean-probs ?prob-series nodata :> ?clean-series)
        (wide->long ?start-idx ?clean-series :> ?period ?prob)
        (r/modis->latlon ?s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
        (date/period->datetime t-res ?period :> ?date)
        (date/convert ?date :year-month-day :year :> ?year))))

(defn mk-tsv
  "Wrapper `for prob-series->tsv-api` handles export to tab-separated text
   via template tap. Header will be \"lat\tlon\tiso\tgadm2\tdate\tprobability\"."
  [est-map forma-path static-path out-path]
  (let [out-fields ["?lat" "?lon" "?iso" "?gadm2" "?date" "?prob"]
        template-fields ["?year" "?iso-extra"]
        src (hfs-seqfile forma-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-textline out-path :sinkmode :replace
                           :sink-template "%s/%s"
                           :templatefields template-fields
                           :outfields out-fields)]
    (?- sink (prob-series->tsv-api est-map src static-src))))
