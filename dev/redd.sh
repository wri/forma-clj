#!/bin/bash
# 
# EMR bootstrap task for our gdal stuff.

set -e

bucket=reddconfig
fwtools=FWTools-linux-x86_64-4.0.0.tar.gz
native=linuxnative.tar.gz
sources=/etc/apt/sources.list

# Install HFD4
sudo chown hadoop $sources
echo "deb http://us-east-1.ec2.archive.ubuntu.com/ubuntu maverick universe  multiverse" > $sources
sudo apt-get update
sudo apt-get install -y --force-yes libhdf4-dev

# FWTOOLS
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$fwtools
sudo mkdir -p /usr/local/fwtools
sudo tar -C /usr/local/fwtools --strip-components=2 -xvzf $fwtools
sudo chown hadoop $fwtools

# Native Bindings
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$native
sudo mkdir -p /home/hadoop/native
sudo tar -C /home/hadoop/native --strip-components=2 -xvzf $native
sudo chown hadoop $native

echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib" >> /home/hadoop/conf/hadoop-user-env.sh
