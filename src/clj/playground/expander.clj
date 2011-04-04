(defn
 seq-avg
 "Gets the average of collection"
 [s]
 (/ (reduce + s) (count s)))

(defn
 simple-expander
 "Expands sequence by inserting new values between each existing element.
  These values are the average of the two values on either side of the
  insertion point.

  Ex.: (simple-expander (range 5))
  => (0 0.5 1 1.5 2 2.5 3 3.5 4)

  "
  [s]
  (cond
      (empty? s) '[]
      :else
          (cons
              (first s)
              (cond
                  (= (count s) 1) (rest s)
                  :else
                     (cons (/ (+ (first s) (second s)) 2.)
                           (simple-expander (rest s)))))))


(defn
  expander
  "Same as simple-expander, except that the interpolation function can be
   customized (defaults to average) and the size of the interpolation window
   can be arbitrarily large (defaults to 1 element on either side of insertion
   point."
  [s n :or [1] f :or [seq-avg] window :or [1]])