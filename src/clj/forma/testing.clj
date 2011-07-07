(ns forma.testing
  "This namespace contains functions that assist in various testing
operations."
  (:use cascalog.api
        midje.sweet
        [cascalog.testing :only (process?-)])
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
  "TODO: FInish docs.

Applies f to each value in coll, splitting it each time f returns
   a new value.  Returns a lazy seq of partitions."
  [pred coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [fst (first s)
           else (first (drop-while (complement pred) (rest s)))
           run (cons fst (take-while (complement pred) (rest s)))]
       (cons (if else (concat run [else]) run)
             (partition-with pred (drop (inc (count run)) s)))))))

(partition-with provided? [1 2 3])
(partition-with provided?
                '("face" "jake" "cake" (provided (ace .a.) => 1)
                  "hell" "jognn" (provided (ace .a.) => 2)
                  ))

(defn  first-elem?  [elem x]
  (and (coll? x)
       (#{elem} (first x))))

(def background? (partial first-elem? 'against-background))
(def provided? (partial first-elem? 'provided))

(defn- reformat
  "deal with the fact that the first item might be a logging level
  keyword. If so, just keep it."
  [[k & more :as bindings]]
  
  (let [[kwd bindings] (if (keyword? k)
                         [k more]
                         [nil bindings])
        [bg bindings] (->> bindings
                           (remove string?)
                           ((juxt filter remove) background?))
        bindings (partition-with provided? bindings)]
    [kwd bg bindings]))

(defn process-results
  [spec query ll]
  (let [ret (if ll
              (process?- ll spec query)
              (process?- spec query))]
    (first (second ret))))

;; TODO: Talk to midje about supporting facts inside of do blocks.
(defn- build-fact?-
  [bindings factor]
  (let [[kwd bg bind] (reformat bindings)]  
    (println kwd ", " bg ", " bind)
    `(do
       ~@(for [bundle bind :let [pre (if (provided? (last bundle))
                                       [(last bundle)]
                                       [])]]
           `(do ~@(for [[spec query] (partition 2 bundle)]
                    `(~factor
                      (process-results ~spec ~query ~kwd) => (just ~spec :in-any-order)
                      ~@pre
                      ~@bg)))))))


(defn- build-fact?<-
  [args factor]
  (let [[kwd [res & body] prereqs] (->> args
                                        (remove string?))
        begin (if kwd [kwd res] [res])]
    `(~factor ~@begin (<- ~@body) ~@prereqs)))

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
