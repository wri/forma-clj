#!/bin/bash
# 
# EMR bootstrap task for our gdal stuff.

set -e

bucket=reddconfig
fwtools=FWTools-linux-x86_64-4.0.0.tar.gz
native=linuxnative.tar.gz
jblas=libjblas.tar.gz
sources=/etc/apt/sources.list
hadoop_lib=/home/hadoop/native/Linux-amd64-64

# Start with screen.
sudo apt-get -y --force-yes install screen
sudo apt-get -y --force-yes install exim4

# Install libhdf4
sudo aptitude -fy install libhdf4-dev

# Install FWTools, (GDAL 1.8.0)
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$fwtools
sudo mkdir -p /usr/local/fwtools
sudo tar -C /usr/local/fwtools --strip-components=2 -xvzf $fwtools
sudo chown --recursive hadoop /usr/local/fwtools

# Download native Java bindings for gdal and jblas
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$native
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$jblas

# Untar everything into EMR's native library path
sudo tar -C $hadoop_lib --strip-components=2 -xvzf $native
sudo tar -C $hadoop_lib --strip-components=1 -xvzf $jblas
sudo chown --recursive hadoop $hadoop_lib

# Add proper configs to hadoop-env.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:$hadoop_lib:\$LD_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-user-env.sh
echo "export JAVA_LIBRARY_PATH=$hadoop_lib:\$JAVA_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-user-env.sh

# Add to bashrc, for good measure.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:$hadoop_lib:\$LD_LIBRARY_PATH" >> /home/hadoop/.bashrc
echo "export JAVA_LIBRARY_PATH=$hadoop_lib:\$JAVA_LIBRARY_PATH" >> /home/hadoop/.bashrc

source /home/hadoop/.bashrc

exit 0
