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

(defn- reformat
  "deal with the fact that the first item might be a logging level
  keyword. If so, just keep it."
  [[one & more :as bindings]]
  (let [[kwd bindings] (if (keyword? one)
                         [one more]
                         [nil bindings])
        bindings (remove string? bindings)]
    (if kwd
      (cons kwd bindings)
      bindings)))

(defn fact?-
  "TODO: Docs. Talk about keyword support."
  [& bindings]
  (doseq [[spec tuples] (->> (reformat bindings)
                             (apply process?-)
                             (apply map vector))]
    (fact tuples => (just spec :in-any-order))))

(defmacro fact?<-
  "TODO: Docs. Talk about how we support only one, for now."
  [& args]
  (let [[begin body] (if (keyword? (first args))
                             (split-at 2 args)
                             (split-at 1 args))]
    `(fact?- ~@begin (<- ~@body))))

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
