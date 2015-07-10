(ns forma.hadoop.jobs.scatter
  "Namespace for arbitrary queries."
  (:use cascalog.api
        [forma.source.tilesets :only (tile-set country-tiles)]
        [forma.hadoop.pail :only (to-pail ?pail- split-chunk-tap)]
        [cascalog.checkpoint :only (workflow)])
  (:require [cascalog.logic.ops :as c]
            [forma.reproject :as r]
            [forma.schema :as schema]
            [forma.hadoop.predicate :as p]
            [forma.hadoop.jobs.forma :as forma]
            [forma.hadoop.jobs.timeseries :as tseries]
            [forma.date-time :as date]
            [forma.classify.logistic :as log]
            [forma.thrift :as thrift]))

(defn forma-run-parameters [k est-end]
  (-> {"1000-32" {:est-start "2005-12-31"
                  :est-end est-end
                  :s-res "1000"
                  :t-res "32"
                  :neighbors 1
                  :window-dims [600 600]
                  :vcf-limit 25
                  :long-block 15
                  :window 5}
       "500-16" {:est-start "2005-12-31"
                 :est-end est-end
                 :s-res "500"
                 :t-res "16"
                 :neighbors 1
                 :window-dims [600 600]
                 :vcf-limit 25
                 :long-block 30
                 :window 10
                 :ridge-const 1e-8
                 :convergence-thresh 1e-6
                 :max-iterations 500
                 :min-coast-dist 3
                 :nodata -9999.0}}
      (get k)))

(defn pail-tap
  "Creates tap for a pail given the pail path, dataset name, and spatial
   and temporal resolution."
  [pail-path dataset s-res t-res]
  (split-chunk-tap pail-path [dataset (format "%s-%s" s-res t-res)]))

(defmain formarunner
  [tmp-root pail-path ts-pail-path fire-pail-path out-path run-key est-end]
  (let [{:keys [s-res t-res est-end] :as est-map} (forma-run-parameters run-key est-end)
        mk-filter (fn [static-path ts-src]
                    (forma/filter-query (hfs-seqfile static-path)
                                        (:vcf-limit est-map)
                                        ts-src))]
    (assert est-map (str run-key " is not a valid run key!"))

    (workflow [tmp-root]

              vcf-step
              ([:tmp-dirs vcf-path]
                 (?- (hfs-seqfile vcf-path)
                     (forma/static-tap (pail-tap pail-path "vcf" s-res "00"))))

              gadm-step
              ([:tmp-dirs gadm-path]
                 (?- (hfs-seqfile gadm-path)
                     (forma/static-tap (pail-tap pail-path "gadm" s-res "00"))))

              hansen-step
              ([:tmp-dirs hansen-path]
                 (?- (hfs-seqfile hansen-path)
                     (forma/static-tap (pail-tap pail-path "hansen" s-res "00"))))

              ecoid-step
              ([:tmp-dirs ecoid-path]
                 (?- (hfs-seqfile ecoid-path)
                     (forma/static-tap (pail-tap pail-path "ecoid" s-res "00"))))

              border-step
              ([:tmp-dirs border-path]
                 (?- (hfs-seqfile border-path)
                     (forma/static-tap (pail-tap pail-path "border" s-res "00"))))

              static-step
              ([:tmp-dirs static-path]
                 (?- (hfs-seqfile static-path)
                     (forma/consolidate-static (:vcf-limit est-map)
                                               (hfs-seqfile vcf-path)
                                               (hfs-seqfile gadm-path)
                                               (hfs-seqfile hansen-path)
                                               (hfs-seqfile ecoid-path)
                                               (hfs-seqfile border-path))))

              stop-process
              ([]
                 "stop everything before deleting the temp directory"
                 (?- (hfs-seqfile "/mnt/hgfs/Dropbox/yikes")
                     (hfs-seqfile "/mnt/hgfs/Dropbox/yikestimes"))))))
