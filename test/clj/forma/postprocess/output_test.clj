(ns forma.postprocess.output-test
  (:use [forma.postprocess.output] :reload)
  (:use [midje sweet]))

(facts "Test `backward-looking-mavg`"

  ;; Test for a small example
  (backward-looking-mavg 3 [1 2 3 4]) => [1.0 1.5 2.0 3.0]
  
  (let [series (map double (range 10))]    
    ;; The length of the series shouldn't change irrespective of the
    ;; window length, since the moving average is backwards-looking.
    (count (backward-looking-mavg 3 series)) => (count series)
    (count (backward-looking-mavg 8 series)) => (count series)

    ;; Likewise, the value of first element should remain unchanged in
    ;; the series (except that it may change to a Double).
    (first (backward-looking-mavg 3 series)) => (first series)))

(facts "Test `clean-probs`"
  (let [ts [0.05 0.1 0.15 0.2 0.25 0.3 0.35 0.4]
        cleaned-ts (clean-probs ts -9999.0)]
    
    cleaned-ts => [[5 8 10 15 20 25 30 35]]
    
    ;; The value of the first cleaned timeseries should equal 100
    ;; times the value of the original timeseries
    (ffirst cleaned-ts) => ((comp int #(* 100 %) first) ts)

    ;; Check that the length of the nested vector is equal to the
    ;; length of the original timeseries of probabilities.
    (count (first cleaned-ts)) => (count ts))

  (let [ts [-9999.0 0.1 0.15 -9999.0 0.25 0.3 0.35 0.4]]
    (clean-probs ts -9999.0)) => [[0 5 8 13 18 23 30 35]]

  (let [ts [0.05 0.1 0.15 -9999.0 0.25 0.3 0.35 0.4]]
    (clean-probs ts -9999.0))  => [[5 8 10 13 18 23 30 35]])
