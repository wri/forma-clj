(ns forma.source.gadmiso-test
  (:use [forma.source.gadmiso] :reload)
  (:use [midje sweet]))

(facts "checks the return value of supplied GADMs"
  (gadm->iso 1024)  => "ARG"
  (gadm->iso 2048)  => "AUS"
  (gadm->iso 16885) => "ITA")
