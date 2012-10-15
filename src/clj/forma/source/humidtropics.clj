(ns forma.source.humidtropics
  (:require [clojure.java.io :as io]))

(def eco-id-seq
  "Returns a sequence of vectorized strings (e.g., [\"AFG,1\"])
  from the admin-map located in the resources folder."
  (-> "humid-tropics-ids.csv" io/resource io/reader line-seq))

(def humid-tropics-ids
  (set (map read-string eco-id-seq)))

(defn in-humid-tropics?
  [ecoid]
  (contains? humid-tropics-ids ecoid))
