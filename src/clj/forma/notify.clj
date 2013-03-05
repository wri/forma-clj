(ns forma.notify
  (:use [clj-aws.core]
        [clj-aws.ses])
  (:require [clojure.java.io :as io]))

(defn send-dummy-email []
  (let [clnt (client (credentials "aws-id" "aws-secret-key"))
        from "someone@somewhere.com"
        dst (destination ["someone_else@somewhere.com"] :cc ["cc_me@somewhere.com"])
        msg (message "subject" "body")]
    (send-email clnt from dst msg)))
