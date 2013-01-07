#!/bin/bash
# 
# setup script for GDAL and JBLAS, for development and local testing

set -e

sudo apt-get update

bucket=reddconfig
fwtools=FWTools-linux-x86_64-4.0.0.tar.gz
gdal=linuxnative.tar.gz
jblas=libjblas.tar.gz
native_lib="/home/$USER/native/Linux-amd64-64"

mkdir -p $native_lib

# Install helper libraries
sudo apt-get -y install libhdf4-dev
sudo apt-get install libjpeg62-dev
sudo apt-get install libgfortran3


# Install FWTools, (GDAL 1.8.0)
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$fwtools
sudo mkdir -p /usr/local/fwtools
sudo tar -C /usr/local/fwtools --strip-components=2 -xvzf $fwtools
sudo chown --recursive $USER /usr/local/fwtools

# Download native Java bindings for gdal and jblas
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$gdal
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$jblas

# Untar everything into native library path
sudo tar -C $native_lib --strip-components=2 -xvzf $gdal
sudo tar -C $native_lib --strip-components=1 -xvzf $jblas
sudo chown --recursive $USER $native_lib

# add proper configs to .bashrc
echo "export LD_LIBRARY_PATH=/home/$USER/native/Linux-amd64-64/" >> /home/$USER/.bashrc
echo 'export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:$LD_LIBRARY_PATH' >> /home/$USER/.bashrc
echo 'export JAVA_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_LIBRARY_PATH' >> /home/$USER/.bashrc
