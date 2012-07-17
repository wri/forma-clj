(ns forma.testing "These functions provide assistance for directory
navigation for testing. One compromise we make here is the binding of
`dev-resources-subdir`; This is defined as `:dev-resources-path` in
`project.clj`, but reading either of these would force a dependency on
pallet or cake. We've included a test to make sure that `dev` exists
on any deployment of the given project; a failing test means that this
directory should be created."
  (:use cascalog.api))

(def dev-resources-subdir "/dev")

(defn get-current-directory
  "Returns the current project directory."
  []
  (. (java.io.File. ".") getCanonicalPath))

(defn project-path
  "Accepts a sub-path within the current project structure and returns
  a fully qualified system path.

  Example usage:
    (project-path \"/dev/file.txt\")"
  ([] (project-path ""))
  ([sub-path]
     (str (get-current-directory) sub-path)))

(defn dev-path
  "Returns the path within the project to the dev path or a
  subdirectory within dev.

  Example usage:
    (dev-path) => \"forma-clj/dev\"
    (dev-path \"/test.txt\") => \"forma-clj/dev/test.txt\""
  ([]
     (dev-path ""))
  ([sub-path]
     (project-path (str dev-resources-subdir
                        sub-path))))

(defn test-path
  "Returns the path within the project where the small-sample test
  data is stored."
  ([]
     (dev-path "/testdata/smallsample-testdata/"))
  ([sub-path]
     (dev-path (str "/testdata/smallsample-testdata/" sub-path))))
