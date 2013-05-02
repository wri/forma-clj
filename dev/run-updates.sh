#! /bin/bash

################
# Configurable #
################

SRES="500"
TRES="16"
YEAR=$(date +%Y)
MODISLAYERS="[:ndvi]" # :reli
TILES="[:all]" # "[[28 8]]"
ESTSTART="2013-02-18"
ESTEND="2013-03-06"

####################
# Storage settings #
####################

TMP="/tmp"
STAGING="s3n://formastaging"
STATIC="s3n://pailbucket/all-static-seq/all"
ARCHIVE="s3n://modisfiles"
S3OUT="s3n://formatemp/output/may-2013"
PAILPATH="s3n://pailbucket/all-master"
BETAS="s3n://pailbucket/all-betas-APR13"

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
$LAUNCHER "$PREPROCESSNS.PreprocessModis" "$STAGING/MOD13A1/" $PAILPATH "{20}*" "$TILES" $MODISLAYERS

# 57 minutes w/5 high-memory for all data
fireoutput="$S3OUT/fires"
$LAUNCHER "$PREPROCESSNS.PreprocessFire" "$ARCHIVE/fires" $fireoutput 500 16 2000-11-01 $ESTSTART $ESTEND "$TILES"

# 7 minutes w/5 high-memory for all data
# 1h15 w/1 large instance for 1 tile
$LAUNCHER "$PREPROCESSNS.PreprocessRain" "$ARCHIVE/PRECL" "$TMP/rain" $SRES $TRES

# 50 minutes w/5 high-memory for all data - one process took forever,
# had to kill it. Default # of tasks now greater.
# 4h32 with 1 large instance for 1 tile
rainoutput="$S3OUT/rain"
$LAUNCHER "$PREPROCESSNS.ExplodeRain" "$TMP/rain" $rainoutput $SRES "$TILES"


####################
# REST OF WORKFLOW #
####################

# ndvi timeseries
# 12 minutes, 1 large instance, 1 tile
output="$S3OUT/ndvi-series"
#$LAUNCHER forma.hadoop.jobs.timeseries.ModisTimeseries $PAILPATH $output $SRES $TRES ndvi

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

# trends->pail

trends=$output
output=$PAILPATH
$LAUNCHER forma.hadoo.jobs.runner.TrendsPail $SRES $TRES $ESTEND $trends $output

# merge trends

trendspail=$output
output="$TMP/merged-trends"
$LAUNCHER forma.hadoop.jobs.runner.MergeTrends $SRES $TRES $ESTEND $trendspail $output

# forma-tap

dynamic=$output
output="$TMP/forma-tap"
$LAUNCHER forma.hadoop.jobs.runner.FormaTap $SRES $TRES $ESTEND $fireoutput $dynamic $output

# neighbors

dynamic=$output
output="$TMP/neighbors"
$LAUNCHER forma.hadoop.jobs.runner.NeighborQuery $SRES $TRES $dynamic $output

# forma-estimate

dynamic=$output
output="$S3OUT/estimated"
$LAUNCHER forma.hadoop.jobs.runner.EstimateForma $SRES $TRES $BETAS $dynamic $STATIC $output

# probs-pail

dynamic=$output
output=$PAILPATH
$LAUNCHER forma.hadoop.jobs.runner.ProbsPail $SRES $TRES $ESTEND $dynamic $output

# merge-probs

dynamic=$output
output="$S3OUT/merged-estimated"
$LAUNCHER forma.hadoop.jobs.runner.MergePail $SRES $TRES $ESTEND $dynamic $output
