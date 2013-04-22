#! /bin/bash

################
# Configurable #
################

SRES="500"
TRES="16"
YEAR=$(date +%Y)
MODISLAYERS="[:ndvi]" # :reli
TILES="[[28 8]]" # leave blank for all tiles
ESTSTART="2006-01-17"
ESTEND="2006-02-18"

####################
# Storage settings #
####################

TMP="s3n://formatest/updates/tmp"
STAGING="s3n://formatest/updates"
STATIC="s3n://pailbucket/all-static-seq/all"
ARCHIVE="s3n://formatest/updates"
S3OUT="$TMP"  # "s3n://formatest/output"
PAILPATH="s3n://formatest/tmp/pail"

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
$LAUNCHER "$PREPROCESSNS.PreprocessFire" "$ARCHIVE/fires" "$TMP/fires" 500 16 2000-11-01 $ESTSTART $ESTEND "$TILES"

# 7 minutes w/5 high-memory for all data
# 1h15 w/1 large instance for 1 tile
$LAUNCHER "$PREPROCESSNS.PreprocessRain" "$ARCHIVE/PRECL" "$TMP/rain" $SRES $TRES

# 50 minutes w/5 high-memory for all data - one process took forever,
# had to kill it. Default # of tasks now greater.
# 4h32 with 1 large instance for 1 tile
$LAUNCHER "$PREPROCESSNS.ExplodeRain" "$TMP/rain" "$S3OUT/rain-series" $SRES "$TILES"

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

ndvi="$TMP/ndvi-filtered"
rain="$TMP/rain-series"
output="$TMP/adjusted"
$LAUNCHER forma.hadoop.jobs.runner.AdjustSeries $SRES $TRES $ndvi $rain $output

# trends

adjusted=$output
output="$TMP/trends"
$LAUNCHER forma.hadoop.jobs.runner.Trends $SRES $TRES $ESTEND $adjusted $output $ESTSTART

# trends->pail

trends=$output
output=$PAILPATH
$LAUNCHER forma.hadoo.jobs.runner.TrendsPail $SRES $TRES $ESTEND $trends $output

# merge trends

trendspail=$output
output="$TMP/merged-trends"
$LAUNCHER forma.hadoo.jobs.runner.MergeTrends $SRES $TRES $ESTEND $trendspail $output

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

betas="s3n://formatest/tmp/betas"
dynamic="$TMP/neighbors"
output="$TMP/estimated"
$LAUNCHER forma.hadoop.jobs.runner.EstimateForma $SRES $TRES $betas $dynamic $STATIC $output
