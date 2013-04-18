(ns forma.hadoop.jobs.api
  "We’ll generate an API master dataset after each update. This
  dataset will be vertically partitioned into sequence files using a
  template tap as follows:

  /api/{iso}/{year}"
  (:use cascalog.api
        [forma.source.gadmiso :only (gadm->iso)])
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

(defn prob-series->tsv-api
  "Returns a Cascalog query that that converts output of `forma-estimate` to long form
   according to API spec.

   ?year field is intended for use with template tap. ?iso-extra is
   included because the template tap removes :templatefields
   from :outfields, but we want the iso code included in the output."
  [est-map src static-src]
  (let [nodata (:nodata est-map)
        est-start (:est-start est-map)
        t-res (:t-res est-map)]
    (<- [?lat ?lon ?iso ?iso-extra ?gadm ?date ?year ?prob]
        (src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?prob-series)
        (static-src ?s-res ?mod-h ?mod-v ?sample ?line _ ?gadm _ _ _)
        (gadm->iso ?gadm :> ?iso)
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
   via template tap. Header will be \"lat\tlon\tiso\tgadm\tdate\tprobability\"."
  [est-map forma-path static-path out-path]
  (let [out-fields ["?lat" "?lon" "?iso" "?gadm" "?date" "?prob"]
        template-fields ["?year" "?iso-extra"]
        src (hfs-seqfile forma-path)
        static-src (hfs-seqfile static-path)
        sink (hfs-textline out-path :sinkmode :replace
                           :sink-template "%s/%s"
                           :templatefields template-fields
                           :outfields out-fields)]
    (?- sink (prob-series->tsv-api est-map src static-src))))
