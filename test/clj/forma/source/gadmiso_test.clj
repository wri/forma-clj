(ns forma.source.gadmiso-test
  (:use [forma.source.gadmiso] :reload)
  (:use [midje sweet cascalog]))

(fact "Test `text-map`."
  (first text-map) => "AFG,1"
  (last text-map) => "UNK,-9999"
  (count text-map) => 39163)

(fact "Test `parse-line`."
  (parse-line "AFG,1") => {1 "AFG"}
  (parse-line "AFG") => (throws NullPointerException))

(fact "Test that `gadm-iso-map` is constructed correctly, works as expected."
  (gadm-iso-map 1) => "AFG"
  (gadm-iso-map -9999) => "UNK")

(fact "Test that `gadm->iso` correctly wraps `gadm-iso-map`."
  (gadm->iso 1) => "AFG"
  (gadm->iso -9999) => "UNK"  )
