(ns forma.conversion
  (:use (clj-time [core :only (date-time year month)])))

;; TODO -- update this for 250 meter data, and take resolution as an
;; input. (I think the period is 16 days.) This is as easy as changing
;; period-length to a map -- then we can call (period-length res) and
;; get the proper value.

;; TODO -- wipe this out, since we don't use that for 250m data.
(def
  ^{:doc "Length of a period, according to FORMA, keyed by MODIS
  resolution."}
  period-length
  {"1000" 32.0
   "250" 16.0})

(def modis-start (date-time 2000 02))

(defn delta [f start end]
  (- (f end) (f start)))

;; TODO -- make this use resolution in some way.
(defn t-diff [res start end]
  (let [year-diff (delta year start end)
        month-diff (delta month start end)]
    (+ (* 12 year-diff) month-diff)))

;; TODO: UPDATE DOCS
(defn datetime->period
   "Converts a given input date into an interval from the MODIS
  beginnings, as specified by modis-start. This function accepts
  DateTime objects, strings that can be coerced into dates, or an
  arbitrary number of integer pieces. (The last of these must be a
  string representation of a resolution, such as \"1000\". See
  clj-time's date-time and from-string for more information."
  [res & int-pieces]
  (t-diff res modis-start (apply date-time int-pieces)))