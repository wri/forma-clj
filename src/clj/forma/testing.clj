(ns forma.testing
  "This namespace contains functions that assist in various testing
operations."
  (:use cascalog.api)
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

(defn to-stdout
  "Prints all tuples produced by the supplied generator to the output
  stream."
  [gen]
  (?- (stdout) gen))

(defn sequify
  "Returns a sequence containing the tuples produced by the supplied generator."
  [gen & [num-fields]]
  (let [fields (if num-fields
                 (v/gen-nullable-vars num-fields)
                 (get-out-fields gen))]
    (??<- fields (gen :>> fields))))

(defbufferop tuples->string
  "Returns a string representation of the tuples input to this
  buffer."
  [tuples]
  [(apply str tuples)])
