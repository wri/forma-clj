(use '[forma.hadoop.jobs.forma :only (consolidate-timeseries)])

(defn abs-change
  [[a b]]
  (/ (- b a) (clojure.math.numeric-tower/abs a)))

(defn gen-deltas [series]
  ;; [[4 5 6 7 8]]
  [(map abs-change (partition 2 1 series))])

(let [h 28
      v 9
      source-pattern "{28_"
      src [["1.1 1.2 5 2015-01-01"]
           ["1.1 1.2 5 2015-02-02"]]]
  (??- (forma-cleanup src)))
;; forest cover threshold (30%)
;; loss-year threshold (?% of MODIS pixel)


(defn forma-cleanup
  [src]
  (let [t-res "16"
        s-res "250"
        nodata -9999.0
        start-date "2006-01-01"
        start-period (date/datetime->period t-res start-date)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series]
        (src ?textline)
        (clojure.string/split ?textline #"\s" :> ?lon-str ?lat-str ?val-str ?date-str)
        (read-string ?lon-str :> ?lon)
        (read-string ?lat-str :> ?lat)
        (read-string ?val-str :> ?val)
        (r/latlon->modis s-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (p/add-fields s-res :> ?s-res)
        (date/datetime->period t-res ?date-str :> ?period)
        (consolidate-timeseries nodata ?period ?val :> ?pd-series ?series)
        (first ?pd-series :> ?start-idx))))

(use 'forma.hadoop.jobs.postprocess)
(require '[forma.utils :as u])
(in-ns 'forma.hadoop.jobs.postprocess)

(defn filter-forma
  [src thresh t-res replace-val1 replace-val2 nodata]
  (let [q (<- [?lat ?lon ?hit-date ?count]
              (src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series)
              (r/modis->latlon ?s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
              (u/replace-all* replace-val1 nodata ?series :> ?somewhat-clean-series)
              (u/replace-all* replace-val2 nodata ?somewhat-clean-series :> ?clean-series)
              (first-hit thresh ?clean-series :> ?first-hit-idx)
              (+ ?start-idx ?first-hit-idx :> ?period)
              (date/period->datetime t-res ?period :> ?hit-date)
              (c/count :> ?count))]
    (<- [?lat ?lon ?hit-date]
        (q ?lat ?lon ?hit-date _))))

(defn filter-forma-bad
  [src thresh t-res]
  (let [q (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series ?first-hit-idx ?count]
              (src ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series)
              (first-hit thresh ?series :> ?first-hit-idx)
              (string? ?first-hit-idx)
              (c/count :> ?count))]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series ?first-hit-idx]
        (q ?s-res ?mod-h ?mod-v ?sample ?line ?start-idx ?series ?first-hit-idx _))))


(let [src (hfs-seqfile "s3n://forma250/forma-series-2006-14/" :sinkmode :replace)
      sink (hfs-textline "s3n://forma250/forma-filtered-1-text" :sinkmode :replace)
      thresh 1
      t-res "16"
      inf (symbol 'inf)
      neg-inf (symbol '-inf)
      nodata -9999.0]
  (?- sink (filter-forma src thresh t-res inf neg-inf nodata)))

(let [s-res "250"
      forma-src [[28 8 0 0 893 [1 2 3]]]
      gadm2-src [[28 8 0 0 10]]
      static-src [[28 8 0 0 0 0 2801 1 0]]]
  (??- (probs-gadm2-eco-hansen forma-src gadm2-src static-src s-res)))

(defn probs-gadm2-eco-hansen
  [forma-src gadm2-src static-src s-res]
  (let [q (<- [?s-res ?mod-h ?mod-v ?sample ?line ?mod-h500 ?mod-v500 ?sample500 ?line500 ?start-final ?merged-series ?gadm2 ?ecoid ?hansen]
              (forma-src ?mod-h ?mod-v ?sample ?line ?start-final ?merged-series)
              (r/modis->latlon s-res ?mod-h ?mod-v ?sample ?line :> ?lat ?lon)
              (r/latlon->modis "500" ?lat ?lon :> ?mod-h500 ?mod-v500 ?sample500 ?line500)
              (gadm2-src _ ?mod-h500 ?mod-v500 ?sample500 ?line500 ?gadm2)
              (static-src _ ?mod-h500 ?mod-v500 ?sample500 ?line500 _ _ ?ecoid ?hansen _))]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?start-final ?merged-series ?gadm2 ?ecoid ?hansen]
        (q ?s-res ?mod-h ?mod-v ?sample ?line _ _ _ _ ?start-final ?merged-series ?gadm2 ?ecoid ?hansen))))

(comment
  (use 'forma.hadoop.jobs.forma)
  (in-ns 'forma.hadoop.jobs.forma)


(let [src (hfs-textline "s3n://forma250/text")
      sink (hfs-seqfile "s3n://forma250/forma-series-2006-14" :sinkmode :replace)]
  (?- sink (forma-cleanup src)))

(let [src (hfs-seqfile "s3n://forma250/forma-series-2006-14")
      gadm2-src (hfs-seqfile )
      static-src (hfs-seqfile )
      sink (hfs-seqfile "s3n://forma250/forma-series-2006-14" :sinkmode :replace)]
  (?- sink (forma-cleanup src)))

(?- sink src)
(gen-deltas ?series :> ?delta-series)

(defn blah
  []
  (let [src [[]]]
  (<- []
              ;; index within ?delta-series
        (first-hit thresh ?delta-series :> ?first-delta-hit)
        ;; index within ?series
        (inc ?first-delta-hit :> ?first-hit-idx)
        (+ start-period ?first-hit-idx :> ?alert-period)
        (date/period->datetime t-res ?alert-period :> ?alert-date)
        (nth ?delta-series ?first-delta-hit :> ?diff)
        (p/add-fields s-res :> ?s-res)

        ;; (r/modis->latlon :> ?lat ?lon)
        ;; remove duplicated pixels that might be 
        (c/count ?count)
        (= 1 ?count))))
)

(defn remove-dupes
  "Remove duplicates due to EE export (images 4806 x 4806 pixels
  not 4800 x 4800). We're not retaining either copy, for the sake
  of simplicity right now." 
  [src]
  (let [s-res "250"]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line]
        (src ?lat ?lon ?delta-series)
        (r/latlon->modis s-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (p/add-fields s-res :> ?s-res)
        (c/count ?count)
        (= 1 ?count))))

(defn forest-cleanup
  "Returns a query to calculate percentage of MODIS pixel covered by
  forest in 2000, based on Landsat-scale pixels.

  This is used to determine which Hansen loss-year pixels are actually
  considered forested in the first place. The raw tifs store 'no loss'
  as 0 but do not distinguish between 'no loss' and 'nodata'. This
  query lets us work around that by dropping any pixels with 0% forest
  area."
  [forest-src]
  (let [s-res "250"
        pixel-area (* 250 250)]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?forest-perc]
        (forest-src ?line)
        (clojure.string/split ?line #"\s" :> ?lat-str ?lon-str ?val-str)
        (read-string ?lon-str :> ?lon)
        (read-string ?lat-str :> ?lat)
        (read-string ?val-str :> ?perc-forest)
        (r/latlon->modis s-res ?lat ?lon :> ?mod-h ?mod-v ?sample ?line)
        (* 900 ?perc-forest :> ?forest-meters)
        (c/sum ?forest-meters :> ?total-forest)
        (/ ?total-forest pixel-area :> ?forest-perc)
        (< 0 ?forest-perc))))

(defn hansen-cleanup
  "Returns a query to reproject Hansen data into 250m MODIS pixels and
  count up the number of loss-year values."
  [hansen-src]
  (let [s-res "250"]
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?loss-year ?loss-count]
        (hansen-src ?line)
        (clojure.string/split ?line #"\s" :> ?lat-str ?lon-str ?val-str)
        (read-string ?lon-str :> ?lon)
        (read-string ?lat-str :> ?lat)
        (read-string ?val-str :> ?loss-year)
        (r/latlon->modis s-res ?lat ?lon :> ?s-res ?mod-h ?mod-v ?sample ?line)
        (c/count ?loss-count))))

(defn hansen-merge
  "Returns a query to merge the forestcover and Hansen data sets"
  [hansen-src forest-src]
  (let []
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?loss-year ?loss-count ?forest-perc]
        (hansen-src ?s-res ?mod-h ?mod-v ?sample ?line ?loss-year ?loss-count)
        (forest-src ?s-res ?mod-h ?mod-v ?sample ?line ?forest-perc))))

(defn merge-all
  "This merge will not retain any Hansen-derived pixels that are
  outside the FORMA study area."
  [forma-src hansen-forest-src]
  (let []
    (<- [?s-res ?mod-h ?mod-v ?sample ?line ?delta-series ?loss-year ?loss-count ?forest-perc]
        (forma-src ?s-res ?mod-h ?mod-v ?sample ?line ?delta-series)
        (hansen-src ?s-res ?mod-h ?mod-v ?sample ?line ?loss-year ?loss-count ?forest-perc))))

(defn avg-max-delta-hits
  [src forest-thresh start-date extract-start extract-end loss-count-thresh]
  (<- [?loss-year ?avg-max-delta]
      (src ?s-res ?mod-h ?mod-v ?sample ?line ?delta-series ?loss-year ?loss-count ?forest-perc)
      (<= forest-thresh ?forest-perc)
      (< loss-count-thresh ?loss-count)
      ;; function that extracts deltas for specific date range
      (extractor ?delta-series start-date extract-start extract-end :> ?mini-series)
      (max ?mini-series :> ?max-delta)
      (c/avg ?max-delta :> ?avg-max-delta)))

(defn avg-max-delta-nonhits
  [src forest-thresh start-date extract-start extract-end loss-count-thresh]
  (<- [?loss-year ?avg-max-delta]
      (src ?s-res ?mod-h ?mod-v ?sample ?line ?delta-series ?loss-year ?loss-count ?forest-perc)
      ;; already screened out "nodata" values - these are forested
      ;; pixels that have not experienced any loss
      (= 0 ?loss-year)
      (<= forest-thresh ?forest-perc)
      (< loss-count-thresh ?loss-count)
      ;; function that extracts deltas for specific date range
      (extractor ?delta-series start-date extract-start extract-end :> ?mini-series)
      (max ?mini-series :> ?max-delta)
      (c/avg ?max-delta :> ?avg-max-delta)))

(defn copy-resource-file
  [file-name out-dir]
  (let [out-file (io/file out-dir file-name)
        in-path (.getPath (io/resource file-name))]
  (with-open [in (io/input-stream in-path)]
    (io/copy in out-file))))

(defn safe-delete [file-path]
  (if (.exists (clojure.java.io/file file-path))
    (try
      (clojure.java.io/delete-file file-path)
      (catch Exception e (str "exception: " (.getMessage e))))
    false))

(defn delete-directory-content [directory-path]
  (let [directory-contents (file-seq (clojure.java.io/file directory-path))
        files-to-delete (filter #(.isFile %) directory-contents)]
    (doseq [file files-to-delete]
      (safe-delete (.getPath file)))
    (safe-delete directory-path)))


(comment

  (clojure.java.shell/sh "/usr/bin/python" "/home/robin/Dropbox/code/github/wri/forma-update/export_tifs.py" "/home/robin/Downloads/export2011_10-10-0000000000-0000000000.tif" "/tmp/")




(let [py "dev/export_tifs.py"
      src [["/home/robin/Downloads/export2011_10-10-0000000000-0000000000.tif"]]
      outdir "/tmp/"]
  (<- [?output]
      (src ?path)
      (clojure.java.shell/sh "/usr/bin/python" py ?path outdir :> ?output)))


(use 'cascalog.api) 

(let [python "/usr/bin/python"
      py "/home/hadoop/export_tifs.py"
      src (hfs-textline "s3n://forma250/tifs")
      remote-dir "/text/"
      bucket "forma250"
      outdir "/mnt/"
      sink (hfs-textline "s3n://forma250/logs" :sinkmode :replace)]
  (?<- sink [?output]
      (src ?http-path)

      (clojure.java.shell/sh python py ?http-path outdir bucket remote-dir :> ?output)))

  (use 'forma.hadoop.jobs.forma)
  (in-ns 'forma.hadoop.jobs.forma)
  (require '[clojure.java.io :as io])

  (let [src (hfs-textline "s3n://forma250/tifs")
      sink (hfs-textline "s3n://forma250/fix-python" :sinkmode :replace)]
  (?<- sink [?output]
      (src ?dummy)
      (copy-resource-file "export_tifs.py" "/home/hadoop/" :> ?output)
;;      (delete-directory-content "/mnt/" :> ?del-output)
      )))
