#! /bin/bash

################
# Configurable #
################

SRES="500"
TRES="16"
#YEAR=$(date +%Y) # no longer used
MODISLAYERS="[:ndvi]" # :reli
TILES="[[28 8]]" # leave blank for all tiles
ESTSTART="2005-12-19"
ESTEND="2013-03-06"

####################
# Storage settings #
####################

TMP="/tmp"
STAGING="s3n://formastaging/"
STATIC="s3n://pailbucket/all-static-seq/all"
ARCHIVE="s3n://modisfiles/"
S3OUT="s3n://formatemp/output/"
PAILPATH="s3n://pailbucket/all-master"
BETAS="s3n://formatest/tmp/betas"

#############
# Constants #
#############

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
$LAUNCHER "$PREPROCESSNS.PreprocessModis" "$STAGING/MOD13A1/" $PAILPATH "{*}" "$TILES" $MODISLAYERS

# 57 minutes w/5 high-memory for all data
fireoutput="$S3OUT/fires"
$LAUNCHER "$PREPROCESSNS.PreprocessFire" "$ARCHIVE/fires" $fireoutput 500 16 2000-11-01 $ESTSTART $ESTEND "$TILES"

# 7 minutes w/5 high-memory for all data
# 1h15 w/1 large instance for 1 tile
$LAUNCHER "$PREPROCESSNS.PreprocessRain" "$ARCHIVE/PRECL" "$TMP/rain" $SRES $TRES

# 50 minutes w/5 high-memory for all data - one process took forever,
# had to kill it. Default # of tasks now greater.
# 4h32 with 1 large instance for 1 tile
rainoutput="$S3OUT/rain-series"
$LAUNCHER "$PREPROCESSNS.ExplodeRain" "$TMP/rain" rainoutput $SRES "$TILES"

####################
# REST OF WORKFLOW #
####################

# ndvi timeseries
# 12 minutes, 1 large instance, 1 tile
output="$TMP/ndvi-series"
$LAUNCHER forma.hadoop.jobs.timeseries.ModisTimeseries $PAILPATH $output $SRES $TRES ndvi

# ndvi filter

ts=$output
output="$TMP/ndvi-filtered"
$LAUNCHER forma.hadoop.jobs.runner.TimeseriesFilter $SRES $TRES $ts $STATIC $output

# join, adjust series

ndvi=$output
output="$TMP/adjusted"
$LAUNCHER forma.hadoop.jobs.runner.AdjustSeries $SRES $TRES $ndvi $rainoutput $output

# trends

adjusted=$output
output="$TMP/trends"
$LAUNCHER forma.hadoop.jobs.runner.Trends $SRES $TRES $ESTEND $adjusted $output $ESTSTART

# forma-tap

dynamic=$output
output="$TMP/forma-tap"
$LAUNCHER forma.hadoop.jobs.runner.FormaTap $SRES $TRES $ESTEND $fireoutput $dynamic $output

# neighbors

dynamic=$output
output="$TMP/neighbors"
$LAUNCHER forma.hadoop.jobs.runner.NeighborQuery $SRES $TRES $dynamic $output

# forma-estimate

dynamic="$TMP/neighbors"
output="$TMP/estimated"
$LAUNCHER forma.hadoop.jobs.runner.EstimateForma $SRES $TRES $BETAS $dynamic $STATIC $output
