#! /bin/bash

################
# Configurable #
################

SRES="500"
TRES="16"
YEAR=$(date +%Y)
MODISLAYERS="[:ndvi]" # :reli
TILES=":all" # :all for all tiles, otherwise a la [[28 8]]
ESTSTART="2013-01-01"
ESTEND="2013-01-17"

####################
# Storage settings #
####################

TMP="/mnt"
STAGING="s3n://formastaging/"
STATIC="s3n://pailbucket/all-static-seq/all"
ARCHIVE="s3n://formatest/updates"
S3OUT="s3n://formatest/updates/out/jan2013"
PAILPATH="s3n://pailbucket/all-master"
TRENDSPATH="$TMP/trends"

#############
# Constants #
#############
BETAS="s3n://formatest/tmp/betas"
FORMAJAR="target/forma-0.2.0-SNAPSHOT-standalone.jar"
LAUNCHER="hadoop jar $FORMAJAR"
PREPROCESSNS="forma.hadoop.jobs.preprocess"
FORMANS="forma.hadoop.jobs.forma"

#################
# PREPROCESSING #
#################

# static preprocessing
# skip for now #


## Preprocess MODIS data
# 5 minutes w/5 high-memory for 2 periods
#$LAUNCHER "$PREPROCESSNS.PreprocessModis" "$STAGING/MOD13A1/" $PAILPATH "{2013*}" "$TILES" $MODISLAYERS

# 57 minutes w/5 high-memory for all data
#$LAUNCHER "$PREPROCESSNS.PreprocessFire" "$STAGING/fires" "$S3OUT/fires" 500 16 2000-11-01 $ESTSTART $ESTEND "$TILES"

# 7 minutes w/5 high-memory for all data
# 1h15 w/1 large instance for 1 tile
#$LAUNCHER "$PREPROCESSNS.PreprocessRain" "$STAGING/PRECL" "$TMP/rain" $SRES $TRES

# 50 minutes w/5 high-memory for all data - one process took forever,
# had to kill it. Default # of tasks now greater.
# 4h32 with 1 large instance for 1 tile
#$LAUNCHER "$PREPROCESSNS.ExplodeRain" "$TMP/rain" "$S3OUT/rain-series" $SRES "$TILES"

####################
# REST OF WORKFLOW #
####################

# ndvi timeseries
# 12 minutes, 1 large instance, 1 tile
output="$TMP/ndvi-series"
#$LAUNCHER forma.hadoop.jobs.timeseries.ModisTimeseries $PAILPATH $output $SRES $TRES ndvi

# ndvi filter

ts=$output
output="$TMP/ndvi-filtered"
#$LAUNCHER forma.hadoop.jobs.runner.TimeseriesFilter $SRES $TRES $ts $STATIC $output

# join, adjust series

ndvi="$TMP/ndvi-filtered"
rain="$TMP/rain-series"
output="$TMP/adjusted"
#$LAUNCHER forma.hadoop.jobs.runner.AdjustSeries $SRES $TRES $ndvi $rain $output

# trends

adjusted="s3n://formatest/updates/tmp/jan2013/adjusted"
output="$S3OUT/trends"
#$LAUNCHER forma.hadoop.jobs.runner.Trends $SRES $TRES $ESTEND $adjusted $output $ESTSTART

# forma-tap

fires="$TMP/fires"
dynamic=$output
output="$TMP/forma-tap"
$LAUNCHER forma.hadoop.jobs.runner.FormaTap $SRES $TRES $ESTEND $fires $dynamic $output

# neighbors

dynamic=$output
output="$TMP/neighbors"
$LAUNCHER forma.hadoop.jobs.runner.NeighborQuery $SRES $TRES $dynamic $output

# forma-estimate

dynamic="$TMP/neighbors"
output="$S3OUT/estimated"
$LAUNCHER forma.hadoop.jobs.runner.EstimateForma $SRES $TRES $BETAS $dynamic $STATIC $output
