## Static Dataset Resolution ##

For static dataset sampling, we've decided to go ahead and chunk at
1km resolution, just to get this initial dataset out. It probably
makes more sense for these static datasets to chunk at 250m, and
sample up when necessary.

We'll have to maintain 16x the data to do this, BUT, because these
datasets are static, this amount will never change. Compare to 130
months at each resolution for the dynamic datasets.... no big deal.

### Static <-> MODIS conversion ###

If a WGS84 dataset has a higher resolution than a modis dataset, we
need to downsample the thing -- this requires `(convert ?row ?col :>
?mod-h ?mod-v ?sample ?line)`. We assume that if we do this, we're
going to have DUPLICATE samples and lines... we'll need to take in a
function for aggregation as well.

```clojure
    (<- [?mod-h ?mod-v ?sample ?line ?avg]
        (wgs84-src ?row ?col ?val)
        (sample [tile-seq] ?row ?col :> ?mod-h ?mod-v ?sample ?line)
        (cascalog.ops/avg ?val :> ?avg)) 
```

If we have lower resolution, however, we'll need to manufacture modis
pixel values from a memory source tap, and allow cascalog's implicit
joins to take care of the sampling of the MODIS data:

```clojure
    (<- [?mod-h ?mod-v ?sample ?line ?val]
        (modis-src [tile-seq] ?mod-h ?mod-v ?sample ?line)
        (sample ?mod-h ?mod-v ?sample ?line :> ?row ?col)
        (wgs84-src ?row ?col ?val))
```

## Timeseries Length ##

It's certainly possible that some timeseries (especially sparse
timeseries, like fires) will have holes somewhere in their
range. Right now, we can fill these bad boys in with the sparse
vectoring functions. What do we do if the last values are missing?

* So, for the timeseries function, we should take two endcap dates for
* timeseries generation. If any of the datasets don't match those
* beginning and ending periods, we should stretch them forward and
* back and fill them with empty values, so that we have EQUAL LENGTH
* timeseries for analysis.  Whizbang and whoopbang, or whatever we're
* going to call them, walk along these ever expanding windows and do
* some operation on the lenghtening timeseries. For fire, we just sum
* the damned things, or take the average, for the temperature.

## Fire Aggregation ##

We deal with fires in a very simple way, right now... we have these
hardcoded limits, currently `(> confidence 50)` and `(> temp
330)`. Ideally, these limits should be inputs to the system, and
could be varied in some way.

* Do we have any sort of justification on hand for those cutoffs?

We also maintain, for each month in the final dataset, a cumulative
count of all fires that have happened up to that time period from the
reference point. This makes certain assumptions about the effect of a
fire on a piece of land -- but does a fire 30 years ago really have
anything to do with the current state of things? It seems that fires
should be some sort of moving window count. If an area is particularly
prone to fires, that count is going to stay high, and consistent... if
some big event happened, the effect will fall off, which makes sense.

Could our model tune itself to take these effects into account?

* Does a running count make any sense?

We also make a few assumptions when moving from daily temporal
resolution up to monthly. Currently, we just maintain each count
(total fires, confidence about 50, etc), and sum the whole business
up. We don't keep track of any information about the temperature! Just
the cutoffs. Is this correct?

## Moving Windows ##

Edges! We don't have any way to deal with the edges of windows, as of
yet. I think I have some ideas for this, but we haven't really tackled
it yet.

### Temporal Conversions ###

It'd be great if we could have a macro that allowed us to chain
temporal conversions! We'd have to define a number of elements, with
conversions between them, and maintain that with heavy, heavy testing.
