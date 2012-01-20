(ns forma.matrix.jblas
  (:import [org.jblas FloatMatrix]))

(defn feature-vec [n]
  (FloatMatrix.
   (into-array (for [x (range n)]
                 (float-array (cons 1 (take 22 (repeatedly rand))))))))

(defn test-mult [n]
  (let [xs  (feature-vec n)
        xst (.transpose xs)]
    (time (let [result (.mmul xst xs)]
            [(.rows result)
             (.columns result)]))))

;; user> (test-mult 10000)
;; "Elapsed time: 6.99 msecs"
;; [23 23]

;; user> (test-mult 100000)
;; "Elapsed time: 43.88 msecs"
;; [23 23]

;; user> (test-mult 1000000)
;; "Elapsed time: 383.439 msecs"
;; [23 23]

(defn matrix-stream [rows cols]
  (repeatedly #(FloatMatrix/randn rows cols)))

(defn square-benchmark
  "Times the multiplication of a square matrix."
  [n]
  (let [[a b c] (matrix-stream n n)]
    (time (.mmuli a b c))
    nil))

;; forma.matrix.jblas> (square-benchmark 10)
;; "Elapsed time: 0.113 msecs"
;; nil
;; forma.matrix.jblas> (square-benchmark 100)
;; "Elapsed time: 0.548 msecs"
;; nil
;; forma.matrix.jblas> (square-benchmark 1000)
;; "Elapsed time: 107.555 msecs"
;; nil
;; forma.matrix.jblas> (square-benchmark 2000)
;; "Elapsed time: 793.022 msecs"
;; nil
