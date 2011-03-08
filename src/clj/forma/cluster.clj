(ns forma.cluster
  (:require [pallet.core :as c]
            [pallet.compute :as compute]))

(def ec2-service
  (compute/compute-service "ec2"
                           :identity "AKIAJ2R6HW5XJPY2GM3Q"
                           :credential "CaEB6a7T/7yqEysrvK/V86LDOT6BLRkgXrdkjsev"))

