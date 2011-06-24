(ns forma.utils)

(defn scale
  "Scales each element in a collection of numbers by the supplied
  factor."
  [fact coll]
  (for [x coll] (* x fact)))

(defn running-sum
  "Given an accumulator, an initial value and an addition function,
  transforms the input sequence into a new sequence of equal length,
  increasing for each value."
  [acc init add-func tseries]
  (first (reduce (fn [[coll last] new]
                   (let [last (add-func last new)]
                     [(conj coll last) last]))
                 [acc init]
                 tseries)))
