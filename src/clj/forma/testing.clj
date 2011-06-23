(ns forma.testing
  "This namespace contains functions that assist in various FORMA
testing operations."
  (:use cascalog.api)
  (:require [cascalog.vars :as v]))

(defn to-stdout
  "Prints all tuples produced by the supplied generator to the output
  stream."
  [gen]
  (?- (stdout) gen))

(defn sequify
  "Returns a sequence containing the tuples produced by the supplied generator "
  [gen & [num-fields]]
  (let [fields (if num-fields
                 (v/gen-nullable-vars num-fields)
                 (get-out-fields gen))]
    (??<- fields (gen :>> fields))))

(defbufferop
  ^{:doc "Returns a string representation of the tuples input to this
  buffer. Useful for testing!"}
  tuples->string
  [tuples]
  [(apply str tuples)])
