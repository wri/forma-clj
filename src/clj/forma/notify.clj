(ns forma.notify
  (:use [clj-aws.core]
        [clj-aws.ses])
  (:require [clojure.java.io :as io]
            [clj-time.core :as t]
            [clj-time.format :as f]))

(defn send-dummy-email
  "Handy for remembering the syntax for sending an email."
  []
  (let [clnt (client (credentials "aws-id" "aws-secret-key"))
        from "someone@somewhere.com"
        dst (destination ["someone_else@somewhere.com"] :cc ["cc_me@somewhere.com"])
        msg (message "subject" "body")]
    (send-email clnt from dst msg)))

(def expected-pixels
  "Based on previous full runs, there should be this many pixels
   at the end of the workflow."
  60000000)

(def canned-emails
  "Contains boilerplate subjects and bodies for a number
   of notification emails."
  {:workflow-complete {:subject "Workflow complete"
                       :body "Workflow for %s to %s finished on %s at %s."}
   :step-complete {:subject "Step %s complete"
                   :body "Step '%s' for %s to %s finished on %s at %s."}
   :pixel-count {:subject "Output pixel count"
                 :body "There are %d pixels in the output for %s to %s. There should be %d pixels. This is a difference of %d pixels."}})

(defn extract-date-time
  "Returns a hashmap of the current date and time.

   Usage:
     (extract-date-time (now))
     ;=> {:date \"2013-03-05\", :time \"02:58\"}"
  [dt]
  {:date (f/unparse (f/formatters :date) dt)
   :time (f/unparse (f/formatters :hour-minute) dt)})

(defn workflow-complete-body
  "Returns a string containing the body for a :workflow-complete message.

   Usage:
     (workflow-complete-body \"2006-01-01\" \"2007-01-01\" (t/now))
     ;=> \"Workflow for 2006-01-01 to 2007-01-01 finished on 2013-03-05 at 03:00.\""
  [start-date end-date dt]
  (let [{:keys [date time]} (extract-date-time dt)]
    (-> (canned-emails :workflow-complete)
        (:body)
        (format start-date end-date date time))))

(defn step-complete-body
  "Returns a string containing the body for a :step-complete message.

   Usage:
     (step-complete-body \"2006-01-01\" \"2007-01-01\" \"neighbors\" (t/now))
     ;=> \"Step 'neighbors' for 2006-01-01 to 2007-01-01 finished on 2013-03-05 at 03:00.\""
  [start-date end-date step-name dt]
  (let [{:keys [date time]} (extract-date-time dt)]
    (-> (canned-emails :step-complete)
        (:body)
        (format step-name start-date end-date date time))))

(defn pixel-count-body
  "Returns a string containing the body for a :pixel-count message.

   Usage:
     (pixel-count-body \"2013-02-18\" \"2013-02-18\" 5)
     ;=> \"There are 5 pixels in the output for 2013-02-18 to 2013-02-18. There should be 60000000 pixels. This is a difference of 59999995 pixels.\""
  [start-date end-date pixel-count]
  (let [diff (- expected-pixels pixel-count)]
    (-> (canned-emails :pixel-count)
        (:body)
        (format pixel-count start-date end-date expected-pixels diff))))


