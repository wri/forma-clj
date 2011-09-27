#!/bin/bash
# 
# EMR bootstrap task for our gdal stuff.

set -e

bucket=reddconfig
fwtools=FWTools-linux-x86_64-4.0.0.tar.gz
native=linuxnative.tar.gz
sources=/etc/apt/sources.list

# Start with screen.
sudo apt-get -y --force-yes install screen

# Install HFD4
sudo aptitude update
sudo aptitude safe-upgrade -y
sudo aptitude install expect

VAR=$(expect -c '
spawn sudo aptitude install -y libc-bin
expect "Services to restart for GNU libc library upgrade:"
send "\r"
expect "Current status:"
send "\r"
expect eof
')
echo "$VAR"
echo "Sleeping for 5 seconds..."
sleep 5
sudo aptitude install -y libhdf4-dev

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

# Add proper configs to hadoop-env.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:\$LD_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-env.sh
echo "export JAVA_LIBRARY_PATH=/home/hadoop/native:\$JAVA_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-env.sh

# Add to bashrc, for good measure.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:\$LD_LIBRARY_PATH" >> /home/hadoop/.bashrc
echo "export JAVA_LIBRARY_PATH=/home/hadoop/native:\$JAVA_LIBRARY_PATH" >> /home/hadoop/.bashrc
