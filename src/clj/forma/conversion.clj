(ns forma.conversion
  (:use (clj-time [core :only (date-time year month)])))

;; TODO -- double check this, do some research, add to doc string.

(def
  ^{:doc "Date of the first valid MODIS."}
  ref-start
  (date-time 2000 02))

(defn delta [f a b] (- (f b) (f a)))

;; TODO -- make this use resolution in some way.
(defn delta-period
  [res start end]
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
  (delta-period res ref-start (apply date-time int-pieces)))