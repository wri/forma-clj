;; TODO: -- talk about our assumptions on what data's coming in to
;; here.  This is where we'll be implementing OLS, using either infer
;; or incanter on the resulting matrices. Again, we should make sure
;; that we're operating on vectors, not seqs. (Turn a seq into a
;; vector be calling vec on it.)

(ns forma.trends
  (:use cascalog.api
        [forma.matrix.utils :only (sparse-vector)]
        [clojure.contrib.math :only (ceil)]
        [clojure.contrib.seq :only (positions)]))

;; ### Time Series Generation
;;
;; The goal here is to write an aggregator that takes in a sequence of
;; tuples of the form <tperiod, pixel-vector>, and returns tuples of
;; the form <pixel-index, min-time, max-time, timeseries>.
;;
;; We assume that we're receiving chunks for every month within the
;; range. We measure pixel-index as the position inside the
;; chunk. For example:
;;
;;     (timeseries [[1 [7 4 2 1]]
;;                  [2 [1 2 3 4]]
;;                  [3 [3 4 3 2]]
;;                  [4 [4 8 7 5]]
;;                  [5 [1 5 3 2]]])
;;     ;=> ([0 1 5 [7 1 3 4 1]]
;;          [1 1 5 [4 2 4 8 5]]
;;          [2 1 5 [2 3 3 7 3]]
;;          [3 1 5 [1 4 2 5 2]])
;;
;; Let's define a function to give us chunks to work with. (This
;; should really be in a separate test file... that's the next
;; project.)

(defn test-chunks
  "Returns a sample input to the timeseries creation buffer, or a
  sequence of 2-tuples, structured as <period, int-array>. Each
  int-array is sized to `chunk-size`; the returned sequence contains
  tuples equal to the supplied value for `periods`."
  [periods chunk-size]
  (for [period (range periods)]
    (vector period
            (int-array chunk-size
                       (range chunk-size)))))

;; Here's my first try at timeseries:
;;
;;     (defn timeseries [tuples]
;;       (let [transpose #(apply map vector %)
;;             [periods chunks] (transpose tuples)]
;;         (map-indexed #(vector %1
;;                               (first periods)
;;                               (last periods)
;;                               (int-array %2))
;;                      (transpose chunks))))
;;
;; One issue with this is that we're mapping through the data three
;; times, which is inefficient. we do a first transpose to get
;; separate the data into periods and chunks -- we can see this, in
;; our destructuring vector, inside the `let` form. We map again to
;; transpose the chunks into timeseries, then again over these to
;; build our results tuples. I tried the following to see the speed
;; difference gained by removing one of these maps:
;;
;;     (defn timeseries [tuples]
;;       (let [[periods chunks] (apply map vector tuples)
;;             periodize (partial vector
;;                                (first periods)
;;                                (last periods))
;;             tupleize (comp periodize int-array vector)]
;;         (map-indexed cons (apply map tupleize chunks))))
;;
;; In this version, we gain about 15% speed. benchmarked with
;;
;;     (def chunks (test-chunks 130 24000))
;;     (time (dotimes [n 10] (count (timeseries chunks))))
;;
;; I get a bit under 24 seconds for the first version, a bit over 20
;; for the second version. For hadoop, we'll go with the more
;; efficient one! I think it ends up looking nicer, too.
;;
;; To be honest, I'm not really sure why this is faster. I had a hunch
;; that moving the call to `tupleize` inside of map (previously, we
;; were using `transpose` here, then `map-indexed`) would speed things
;; up, and it did.

(defbufferop
  ^{:doc "Takes in a number of <t-period, modis-chunk> tuples, sorted
  by time period, and transposes these into (n = chunk-size) 4-tuples,
  formatted as <pixel-idx, t-start, t-end, t-series>, where the
  `t-series` field is represented by an int-array. Entering chunks
  should be sorted in descending order."}
  timeseries [tuples]
  (let [[periods chunks] (apply map vector tuples)
        periodize (partial vector
                           (first periods)
                           (last periods))
        tupleize (comp periodize int-array vector)]
    (map-indexed cons (apply map tupleize chunks))))

;; [This gist](https://gist.github.com/845813) is a solid example of
;; how to get cascalog to sort by time period and provide tuples to
;; our final `timeseries` function. Note again that incoming chunks
;; will be either float or int arrays.

;; ## Walking the Matrix

;; Great for testing what buffers will do; this returns a string
;; representation of the input.

(defbufferop tuples->string
  [tuples]
  [(apply str (map str tuples))])

;; ## Let's figure out how to get the chunks reconstituted.

(def num-lines 20)
(def num-samples 20)

(def points
  (memory-source-tap
   (for [h (range 3)
         s (range num-samples)
         l (range num-lines) :let [val s]]
     [h 1 s l val])))

;; Buffer to generate a sparse-vector from idx, val tuple pairs.
(defbufferop [rebuild-lines [size placeholder]]
  [tuples]
  [[(sparse-vector size tuples placeholder)]])

;; TODO: Work out solution where group-length can unevenly divide.
;;
;; TODO: document the line-length, num-groups business. Right now, if
;; I'm not mistaken, we need num-groups to divide evenly into
;; line-length.

(defn group-lines
  "Function to generate an aggregating predicate macro that stitches
  lines back together. Lines are grouped into `num-groups` total
  blocks. Currently, we require that `num-groups` divide evenly into
  `line-length`."
  [line-length num-groups]
  {:pre [(= 0 (mod line-length num-groups))]}
  (let [group-length (ceil (/ line-length num-groups))]
    (<- [?line ?sample ?val :> ?line-group ?full-line]
        (:sort ?line ?sample)
        (mod ?sample group-length :> ?subidx)
        (quot ?sample group-length :> ?line-group)
        (rebuild-lines [group-length 0] ?subidx ?val :> ?full-line))))

(defn run-line-aggregator
  "Takes a samples and line generator, and stitches lines back
  together. "
  []
  (let [agg-lines (group-lines 20 2)]
    (?<- (stdout)
         [?tile-h ?tile-v ?line ?line-group ?full-line]
         (points ?tile-h ?tile-v ?sample ?line ?val)
         (agg-lines ?line ?sample ?val :> ?line-group ?full-line))))

;; TODO: -- documentation on why we do this. Nearest neighbor analysis links.
(defn walk-matrix
  "Walks along the rows and columns of a matrix at the given window
  size, returning all (window x window) snapshots."
  [m window]
  (mapcat (comp
           (partial apply map vector)
           (partial map (partial partition window 1)))
          (partition window 1 m)))


;; I think that I might be able to tag pixels as "edges", based on a
;; combination of pixel value and length of groups. If I can get all
;; of the edge pixels aggregated together... that would be a big win!
;;
;; Can we extend this to deal with the whole world, by calculating the
;; global pixel sample and line? One issue would be that edges
;; sometimes wouldn't be met be anything on the other side.
;;
;; ACTUALLY -- this is a problem now, and if we solve it, we solve the
;; whole mess. 
