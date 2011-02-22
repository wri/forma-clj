(ns forma.modis)

;; From the [user's guide](http://goo.gl/uoi8p) to MODIS product MCD45
;; (burned area):"The MODIS data are re-projected using an equiareal
;; sinusoidal projection, defined on a sphere of radius 6371007.181 m,
;; and with the Greenwich meridian as the central meridian of the
;; projection." The full MODIS grid has 18 vertical and 36 horizontal
;; tiles. Each tile is subdivided into pixels, based on the dataset's
;; spatial resolution.

(def rho 6371007.181)

(def h-tiles 36)
(def v-tiles 18)

(def pixels-at-res
  {"250" 4800
   "500" 2400
   "1000" 1200})
