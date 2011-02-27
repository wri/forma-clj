;; TODO
;;
;; talk about our assumptions on what data's coming in to here.  This
;; is where we'll be implementing OLS, using either infer or incanter
;; on the resulting matrices. Again, we should make sure that we're
;; operating on vectors, not seqs. (Turn a seq into a vector be
;; calling vec on it.)

;; TODO
;;
;; [this is some good shit](https://gist.github.com/845813) for the
;; function where we actually build the time series. Not that we'll be
;; getting float and int arrays, instead of vectors! (I think we
;; should transpose this business, and convert the output arrays into
;; int or float arrays (for time series storage), since we have custom
;; serializers for those types, and I think incanter can take those as
;; inputs.)
;;
;; One thing we need to check, here -- what if we have multiple time
;; periods and chunks coming into this thing? That could happen, if we
;; accidentally process duplicates.
;;
;; TODO -- SNS notification, if we accidentally process some duplicate
;; records.
;;
;; TODO -- keep a log of all of the files that we've processed into
;; our buckets!

(ns forma.trends
  (:use cascalog.api))

(defbufferop
  ^{:doc "Takes in a number of vectors, pre-sorted by time period,
representing the same MODIS chunk within a dataset, and aggregates
them by building a time series vector of vectors. Entering chunks
should be sorted in descending order."}
  timeseries [chunks]
  [(apply conj [] chunks)])

(defn transpose
  "Matrix transpose. Transforms

     [[1 2 3]
      [4 5 6]
      [7 8 9]]

into:

     [[1 4 7]
      [2 5 8]
      [3 6 9]]

The code works because map applies the given function to the first
  arguments of all the supplied collections, then the second, etc, all
  down the line. The apply allows us to apply this to any number of
  collections. So, we make a vector out of all of the firsts, then the
  seconds, etc."
  [a]
  (vec (apply map vector a)))

(defn walk-matrix
  "Walks along the rows and columns of a matrix at the given window
  size, returning all (window x window) snapshots."
  [m window]
  (mapcat (comp
           (partial apply map vector)
           (partial map (partial partition window 1)))
          (partition window 1 m)))