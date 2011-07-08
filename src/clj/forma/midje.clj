(ns forma.midje
  (:use midje.sweet
        [cascalog.testing :only (process?-)]
        [clojure.contrib.seq-utils :only (positions)]))

(defn first-elem?  [elem x]
  (when (coll? x) (#{elem} (first x))))

(def background? (partial first-elem? 'against-background))
(def provided? (partial first-elem? 'provided))

(defn extract-prereqs [coll]
  (->> coll
       ((juxt filter remove) #(or (background? %)
                                  (provided? %)) )))

(defn process-results
  [spec query ll]
  (let [ret (if ll
              (process?- ll spec query)
              (process?- spec query))]
    (first (second ret))))

(defn- build-fact?- [bindings factor]
  (let [[k & more :as bindings] (remove string? bindings)
        [kwd bindings] (if (keyword? k)
                         [k more]
                         [nil bindings])]
    `(~factor
      ~@(loop [[x y & more :as forms] bindings, res []]
          (cond (not x) res
                (or (provided? x) (background? x)) (recur (rest forms) (conj (vec res) x))
                :else (->> `((process-results ~x ~y ~kwd) => (just ~x :in-any-order))
                           (concat res)
                           (recur more)))))))

(defn- build-fact?<-
  [args factor]
  (let [[ll :as args] (remove string? args)
        [begin args] (if (keyword? ll)
                       (split-at 2 args)
                       (split-at 1 args))
        [reqs body] (extract-prereqs args)]
    `(~factor ~@begin (cascalog.api/<- ~@body) ~@reqs)))

(defmacro fact?- [& bindings] (build-fact?- bindings `fact))
(defmacro fact?<- [& args] (build-fact?<- args `fact?-))

(defmacro future-fact?- [& bindings] (build-fact?- bindings `future-fact))
(defmacro future-fact?<- [& args] (build-fact?<- args `future-fact?-))

(defmacro pending-fact?- [& bindings] (build-fact?- bindings `pending-fact))
(defmacro pending-fact?<- [& args] (build-fact?<- args `pending-fact?-))
