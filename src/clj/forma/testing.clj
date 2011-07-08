(ns forma.testing
  "This namespace contains functions that assist in various testing
operations."
  (:use cascalog.api
        midje.sweet
        [cascalog.testing :only (process?-)]
        [clojure.contrib.seq-utils :only (positions)])
  (:require [cascalog.vars :as v]))

;; ## Directory Management
;;
;; These functions provide assistance for directory navigation for
;; testing. One compromise we make here is the binding of
;; `dev-resources-subdir`; This is defined as `:dev-resources-path` in
;; `project.clj`, but reading either of these would force a dependency
;; on pallet or cake. We've included a test to make sure that `dev`
;; exists on any deployment of the given project; a failing test means
;; that this directory should be created.

(def dev-resources-subdir "/dev")

(defn get-current-directory
  "Returns the current project directory."
  []
  (. (java.io.File. ".") getCanonicalPath))

(defn project-path
  "Accepts a sub-path within the current project structure and returns
  a fully qualified system path. For example:

    (project-path \"/dev/file.txt\")
    ;=> \"/home/sritchie/myproject/dev/file.txt\""
  ([] (project-path ""))
  ([sub-path]
     (str (get-current-directory) sub-path)))

(defn dev-path
  ([] (dev-path ""))
  ([sub-path]
     (project-path (str dev-resources-subdir
                        sub-path))))

;; ## Cascalog Helpers

;; ### Midje Testing

(defn partition-with
  [pred coll]
  (loop [ret [], left coll, [pos & more] (positions pred coll)]
    (if-not pos
      (if (empty? left) ret (conj ret left))
      (recur (conj ret (take (inc pos) left))
             (drop (inc pos) left)
             more))))

(def split-by (juxt filter remove))

(defn first-elem?  [elem x]
  (when (coll? x) (#{elem} (first x))))

(def background? (partial first-elem? 'against-background))
(def provided? (partial first-elem? 'provided))

(defn extract-prereqs [coll]
  (split-by #(or (background? %)
                 (provided? %))
            coll))

(defn- reformat
  "deal with the fact that the first item might be a logging level
  keyword. If so, just keep it."
  [[k & more :as bindings]]
  (let [[kwd bindings] (if (keyword? k)
                         [k more]
                         [nil bindings])
        [bg bindings] (->> bindings
                           (remove string?)
                           (split-by background?))
        bindings (partition-with provided? bindings)]
    [kwd bg bindings]))

(defn process-results
  [spec query ll]
  (let [ret (if ll
              (process?- ll spec query)
              (process?- spec query))]
    (first (second ret))))

(defn- inner-body
  [bundle kwd]
  (->> (for [[spec query] (partition-all 2 bundle)]
           (if (provided? spec)
             `[~spec]
             `((process-results ~spec ~query ~kwd) => (just ~spec :in-any-order))))
       (apply concat)))

(defn- build-fact?-
  [bindings factor]
  (let [crunch (fn [coll x] (concat (apply concat coll) x))
        [kwd bg bind] (reformat bindings)]  
    `(~factor
      ~@(-> (for [bundle bind]
              (inner-body bundle kwd))
            (crunch bg)))))

(defn- build-fact?<-
  [args factor]
  (let [[ll :as args] (remove string? args)
        [begin args] (if (keyword? ll)
                       (split-at 2 args)
                       (split-at 1 args))
        [reqs body] (extract-prereqs args)]
    `(~factor ~@begin (<- ~@body) ~@reqs)))

(defmacro fact?- [& bindings] (build-fact?- bindings `fact))
(defmacro fact?<- [& args] (build-fact?<- args `fact?-))

(defmacro future-fact?- [& bindings] (build-fact?- bindings `future-fact))
(defmacro future-fact?<- [& args] (build-fact?<- args `future-fact?-))

;; Helper Functions

(defn to-stdout
  "Prints all tuples produced by the supplied generator to the output
  stream."
  [gen]
  (?- (stdout) gen))

(defbufferop tuples->string
  "Returns a string representation of the tuples input to this
  buffer."
  [tuples]
  [(apply str tuples)])
