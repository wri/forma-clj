(ns forma.utils)

;; TODO: Test!
(defn thrush [& args]
  (reduce #(%2 %1) args))

(defn nth-in
  "Takes a nested collection and a sequence of keys, and returns the
  value obtained by taking `nth` in turn on each level of the nested
  collection."
  [coll ks]
  (apply thrush coll (for [k ks]
                       (fn [xs] (nth xs k)))))

(defn scale
  "Returns a collection obtained by scaling each number in `coll` by
  the supplied number `fact`."
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
