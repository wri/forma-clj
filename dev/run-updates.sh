#! /bin/bash

################
# Configurable #
################

SRES="500"
TRES="16"
MODISLAYERS="[:ndvi]" # :reli
TILES="[:all]" # "[[28 8]]"
ESTSTART=$1 # "2013-02-18"
ESTEND=$2 # "2013-03-06"

####################
# Storage settings #
####################

TMP="/tmp"
STAGING="s3n://formastaging"
STATIC="s3n://pailbucket/all-static-seq/all"
ARCHIVE="s3n://modisfiles"
S3OUT="s3n://pailbucket/output/run-20`date +%y-%m-%d`" # store output by date of run
PAILPATH="s3n://pailbucket/all-master"
BETAS="s3n://pailbucket/all-betas"

#############
# Constants #
#############

FORMAJAR="target/forma-0.2.0-SNAPSHOT-standalone.jar"
LAUNCHER="hadoop jar $FORMAJAR"
PREPROCESSNS="forma.hadoop.jobs.preprocess"
RUNNERNS="forma.hadoop.jobs.runner"

#################
# PREPROCESSING #
#################

echo "Processing data to generate FORMA from $ESTSTART to $ESTEND" 

# static preprocessing
# skip for now #


echo "Preprocessing MODIS data"
# 5 minutes w/5 high-memory for 2 periods
$LAUNCHER "$PREPROCESSNS.PreprocessModis" "$STAGING/MOD13A1/" $PAILPATH "{20}*" "$TILES" $MODISLAYERS

# 57 minutes w/5 high-memory for all data
echo "Preprocessing fires"
fireoutput="$S3OUT/fires"
$LAUNCHER "$PREPROCESSNS.PreprocessFire" "$ARCHIVE/fires" $fireoutput 500 16 2000-11-01 $ESTSTART $ESTEND "$TILES"

# 7 minutes w/5 high-memory for all data
# 1h15 w/1 large instance for 1 tile
echo "Preprocessing rain"
$LAUNCHER "$PREPROCESSNS.PreprocessRain" "$ARCHIVE/PRECL" "$TMP/rain" $SRES $TRES

# 50 minutes w/5 high-memory for all data - one process took forever,
# had to kill it. Default # of tasks now greater.
# 4h32 with 1 large instance for 1 tile
echo "Exploding rain into MODIS pixels"
rainoutput="$S3OUT/rain"
$LAUNCHER "$PREPROCESSNS.ExplodeRain" "$TMP/rain" $rainoutput $SRES "$TILES"


####################
# REST OF WORKFLOW #
####################

# ndvi timeseries
# 12 minutes, 1 large instance, 1 tile
echo "NDVI timeseries"
output="$TMP/ndvi-series"
$LAUNCHER forma.hadoop.jobs.timeseries.ModisTimeseries $PAILPATH $output $SRES $TRES ndvi

# ndvi filter

echo "Filtering NDVI"
ts=$output
output="$TMP/ndvi-filtered"
$LAUNCHER $RUNNERNS.TimeseriesFilter $SRES $TRES $ts $STATIC $output

# join, adjust series

echo "Joining NDVI and rain series, adjusting length"
ndvi=$output
output="$TMP/adjusted"
$LAUNCHER $RUNNERNS.AdjustSeries $SRES $TRES $ndvi $rainoutput $output

# trends

echo "Calculating trends stats"
adjusted=$output
output="$TMP/trends"
$LAUNCHER $RUNNERNS.Trends $SRES $TRES $ESTEND $adjusted $output $ESTSTART

# trends->pail

echo "Adding trends series to pail"
trends=$output
output=$PAILPATH
$LAUNCHER $RUNNERNS.TrendsPail $SRES $TRES $ESTEND $trends $output

# merge trends

echo "Merging trends time series stored in pail"
trendspail=$output
output="$TMP/merged-trends"
$LAUNCHER $RUNNERNS.MergeTrends $SRES $TRES $ESTEND $trendspail $output

# forma-tap

echo "Prepping FORMA tap for neighbor analysis"
dynamic=$output
output="$TMP/forma-tap"
$LAUNCHER $RUNNERNS.FormaTap $SRES $TRES $ESTEND $fireoutput $dynamic $output

# neighbors

echo "Merging neighbors"
dynamic=$output
output="$TMP/neighbors"
$LAUNCHER $RUNNERNS.NeighborQuery $SRES $TRES $dynamic $output

# forma-estimate

echo "Classify pixels using beta vectors"
dynamic=$output
output="$S3OUT/estimated"
$LAUNCHER $RUNNERNS.EstimateForma $SRES $TRES $BETAS $dynamic $STATIC $output

# probs-pail

echo "Add probability output to pail"
dynamic=$output
output=$PAILPATH
$LAUNCHER $RUNNERNS.ProbsPail $SRES $TRES $ESTEND $dynamic $output

# merge-probs

echo "Merge probability time series, outputting to sequence files in $S3OUT/merged-estimated"
dynamic=$output
output="$S3OUT/merged-estimated"
$LAUNCHER $RUNNERNS.MergeProbs $SRES $TRES $ESTEND $dynamic $output
