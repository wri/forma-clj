#! /bin/bash

# run-updates.sh <run-date> <est-end>
# Date format matches `date +%Y-%m-%d`

TRAININGEND="2005-12-19"
RUNDATE=$1
ESTEND=$2

####################
# Storage settings #
####################

S3OUT="s3n://pailbucket/output/run-$RUNDATE" # store output by date of run
BLUERASTER="s3n://wriforma/$RUNDATE/$TRAININGEND-to-$ESTEND"

## Post-Pre-Processing Tasks

# set paths public
BRPATH=$(echo $BLUERASTER | tr -s "s3n" "s3")
s3cmd setacl $BRPATH --acl-public --recursive

DWPATH=$(echo $S3OUT/david | sed 's/s3n/s3/g')
s3cmd setacl $DWPATH --acl-public --recursive

# notify blue raster about new data
aws ses send-email --subject "FORMA data updated" --from datalab@wri.org --to aallegretti@blueraster.com --text "Please check for new data at $BLUERASTER." --region us-east-1
aws ses send-email --subject "FORMA data updated" --from datalab@wri.org --to cgabris@blueraster.com --text "Please check for new data at $BLUERASTER." --region us-east-1
aws ses send-email --subject "FORMA data updated" --from datalab@wri.org --to cphang@blueraster.com --text "Please check for new data at $BLUERASTER." --region us-east-1

# notify datalab that update is complete
aws ses send-email --subject "FORMA data updated" --from datalab@wri.org --to datalab@wri.org --text "New FORMA data available at $S3OUT." --region us-east-1
aws ses send-email --subject "FORMA data updated" --from datalab@wri.org --to bguzder-williams@wri.org --text "New FORMA data available at $S3OUT." --region us-east-1

# notify David about update
aws ses send-email --subject "FORMA data updated" --from datalab@wri.org --to wheelrdr@gmail.com --text "New FORMA data available at $DWPATH" --region us-east-1
