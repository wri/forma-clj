(ns forma.trends.filter)

;; De-seasonalize time-series

(defn seasonal-rows [n]
  "lazy sequence of monthly dummy vectors"
  (vec
   (take n (partition 11 1 (cycle (cons 1 (repeat 11 0)))))))

(defn seasonal-matrix
  "create a matrix of [num-months]x11 of monthly dummies, where
  [num-months] indicates the number of months in the time-series"
  [num-months]
  (i/bind-columns (ones-column num-months)
                  (seasonal-rows num-months)))

(defn deseasonalize
  "deseasonalize a time-series [ts] using monthly dummies. returns
  a vector the same length of the original time-series, with desea-
  sonalized values."
  [ts]
  (let [avg-seq (repeat (count ts) (average ts))
        X    (seasonal-matrix (count ts))
        fix  (i/mmult (variance-matrix X) (i/trans X) ts)
        adj  (i/mmult (i/sel X :except-cols 0) (i/sel fix :except-rows 0))]
    (i/minus ts adj)))

;; Hodrick-Prescott filter

(defn insert-at
  "insert list [b] into list [a] at index [idx]."
  [idx a b]
  (let [opened (split-at idx a)]
    (concat (first opened) b (second opened))))

(defn insert-into-zeros
  "insert vector [v] into a vector of zeros of total length [len]
  at index [idx]."
  [idx len v]
  (insert-at idx (repeat (- len (count v)) 0) v))

(defn hp-mat
  "create the matrix of coefficients from the minimization problem
  required to parse the trend component from a time-series of le-
  gth [T], which has to be greater than or equal to 9 periods."
  [T]
  {:pre [(>= T 9)]
   :post [(= [T T] (i/dim %))]}
  (let [first-row  (insert-into-zeros 0 T [1 -2 1])
        second-row (insert-into-zeros 0 T [-2 5 -4 1])
        inner (for [x (range (inc (- T 5)))]
                   (insert-into-zeros x T [1 -4 6 -4 1]))]
    (i/matrix
     (concat [first-row]
             [second-row]
             inner
             [(reverse second-row)]
             [(reverse first-row)]))))


(defn hp-filter
  "return a smoothed time-series, given the HP filter parameter."
  [ts lambda]
  (let [T (count ts)
        coeff-matrix (i/mult lambda (hp-mat T))
        trend-cond (i/solve (i/plus coeff-matrix (i/identity-matrix T)))]
    (i/mmult trend-cond ts)))




