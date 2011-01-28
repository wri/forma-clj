;; I want to go ahead and rewrite the code found at this site:
;; http://landweb.nascom.nasa.gov/developers/tilemap/note.html
;; Now, as referenced here:
;; http://www.dfanning.com/map_tips/modis_overlay.html
;; NASA stopped using integerized sinusoidal projection in collection 3, and moved on to strictly
;; sinusoidal in collection 4. I believe that NDVI uses a straight up sinusoidal, so that's all we need to worry about.

;; looking at the code, we only need to deal with sn = Sinusoidal grid
;; k, h or q pixel size, for 1000m, 500m, and 250m.
;; inv, as we're going from projection to lat/long.
;; 