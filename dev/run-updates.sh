#! /bin/bash

################
# Configurable #
################

THRESH=50
VERSION="1.0"
SRES="500"
TRES="16"
CDMTRES="32"
MODISLAYERS="[:ndvi]" # :reli
TILES="[:all]" # "[[28 8] [32 9]"
TRAININGEND="2005-12-19"
ESTSTART=$1 # "2013-02-18"
ESTEND=$2 # "2013-03-06"
FIRESTART="2000-11-01"
SUPERECO=false # by default, do not use super-ecoregions
NODATA=-9999.0

####################
# Storage settings #
####################

TMP="/tmp"
STAGING="s3n://formastaging"
STATIC="s3n://pailbucket/all-static-seq/all"
GADM2="s3n://pailbucket/all-static-seq/vcf-filtered/gadm2"
ARCHIVE="s3n://modisfiles"
S3OUT="s3n://pailbucket/output/run-20`date +%y-%m-%d`" # store output by date of run
PAILPATH="s3n://pailbucket/all-master"
BETAS="s3n://pailbucket/all-betas"
BLUERASTER="s3n://wriforma/20`date +%y-%m-%d`/$TRAININGEND-to-$ESTEND"
DOWNLOAD="s3n://forma/$VERSION/"

#############
# Constants #
#############

FORMAJAR="target/forma-1.0.0-SNAPSHOT-standalone.jar"
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
$LAUNCHER "$PREPROCESSNS.PreprocessModis" "$STAGING/MOD13A1/" $PAILPATH "{20}*" "$TILES" "$MODISLAYERS"

# 7 minutes w/5 high-memory for all data
# 4 minutes w/25 high-memory for all data
# 1h15 w/1 large instance for 1 tile
echo "Preprocessing rain"
rainraw="$ARCHIVE/PRECL"
output="$TMP/rain-raw"
$LAUNCHER "$PREPROCESSNS.PreprocessRain" $rainraw $output $SRES $TRES

# 50 minutes w/5 high-memory for all data - one process took forever,
# had to kill it. Default # of tasks now greater.
# 4h32 with 1 large instance for 1 tile
# Now save to $TMP because the same cluster is used for all steps for updates.
# With 25 high memory instances took 22 minutes, of which 12 were spent
# waiting for copying to s3 to finish.
echo "Exploding rain into MODIS pixels"
rain=$output
rainoutput="$TMP/rain"
$LAUNCHER "$PREPROCESSNS.ExplodeRain" $rain $rainoutput $SRES "$TILES"

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
output="$S3OUT/adjusted"
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
output="$S3OUT/merged-trends"
$LAUNCHER $RUNNERNS.MergeTrends $SRES $TRES $ESTEND $trendspail $output

# 57 minutes w/5 high-memory for all data
# 35 minutes with 25 high memory for all periods & all tiles. Lots of errors reading
# 3gb csv of fires.
# Now saving to tmp instead of S3, because we use the same cluster for the whole
# update process.
echo "Preprocessing fires"
fireoutput="$TMP/fires"
$LAUNCHER "$PREPROCESSNS.PreprocessFire" "$ARCHIVE/fires" $fireoutput 500 16 $FIRESTART "$TILES"

# forma-tap

echo "Prepping FORMA tap for neighbor analysis"
dynamic=$output
output="$TMP/forma-tap"
$LAUNCHER $RUNNERNS.FormaTap $SRES $TRES $ESTSTART $ESTEND $fireoutput $dynamic $output

# neighbors

echo "Merging neighbors"
dynamic=$output
neighbors="$TMP/neighbors"
$LAUNCHER $RUNNERNS.NeighborQuery $SRES $TRES $dynamic $neighbors

# beta-data-prep

echo "Beta data prep - only keep data through training period"
dynamic=$neighbors
output="$TMP/beta-data"
#$LAUNCHER $RUNNERNS.BetaDataPrep $SRES $TRES $dynamic $STATIC $output true

# gen-betas

#echo "Generating beta vectors"
#dynamic=$output
#output="$S3OUT/betas"
#$LAUNCHER $RUNNERNS.GenBetas $SRES $TRES $TRAININGEND $dynamic $output
# if you're re-generating betas, make sure you put them in 
# s3n://pailbucket/all-betas when they're ready

# forma-estimate

echo "Classify pixels using beta vectors"
dynamic=$neighbors
output="$S3OUT/estimated"
betas="$BETAS"
$LAUNCHER $RUNNERNS.EstimateForma $SRES $TRES $betas $dynamic $STATIC $output $SUPERECO

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

# add gadm2 & ecoregion ids

echo "Merging in gadm2 and eco fields"
dynamic=$output
gadm2eco="$S3OUT/merged-estimated-gadm2-eco"
$LAUNCHER $RUNNERNS.ProbsGadm2 $dynamic $GADM2 $STATIC $gadm2eco

# convert to common data model

echo "Converting to common data model"
srcpath=$gadm2eco
output="$S3OUT/cdm"
$LAUNCHER $RUNNERNS.Cdm $THRESH $Z $TRES $CDMTRES $ESTSTART $srcpath $output

# convert to CDM

echo "Converting to common data model"
srcpath=$gadm2eco
output="$BLUERASTER"
$LAUNCHER $RUNNERNS.BlueRaster $THRESH $Z $TRES $CDMTRES $ESTSTART $srcpath $output

# prep data for FORMA download
echo "Prepping data for FORMA download link"
srcpath=$gadm2eco
output=$DOWNLOAD
$LAUNCHER $RUNNERNS.FormaDownload $THRESH $TRES $NODATA $srcpath $output
