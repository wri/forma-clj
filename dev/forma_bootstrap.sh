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

# Upgrading GCC requires a restart of certain services; this is a
# little hacky, but rather than do this manually we'll move the
# services out of init.d, place them back in after the upgrade and
# restart them manually.
sudo mv /etc/init.d/mysql /home/hadoop/mysql
sudo mv /etc/init.d/cron /home/hadoop/cron
sudo mv /etc/init.d/exim4 /home/hadoop/exim4

# Upgrade GCC.
sudo aptitude install -y libc-bin

# Move services back...
sudo mv /home/hadoop/mysql /etc/init.d/mysql 
sudo mv /home/hadoop/cron /etc/init.d/cron 
sudo mv /home/hadoop/exim4 /etc/init.d/exim4 

# and restart.
sudo /etc/init.d/mysql restart
sudo /etc/init.d/cron restart
sudo /etc/init.d/exim4 restart

# Install libhdf4
sudo aptitude -fy install libhdf4-dev

# Install FWTools, (GDAL 1.8.0)
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$fwtools
sudo mkdir -p /usr/local/fwtools
sudo tar -C /usr/local/fwtools --strip-components=2 -xvzf $fwtools
sudo chown --recursive hadoop /usr/local/fwtools

# Install Native Java Bindings
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$native
wget -S -T 10 -t 5 http://$bucket.s3.amazonaws.com/$jblas
sudo mkdir -p /home/hadoop/native
sudo tar -C $hadoop_lib --strip-components=2 -xvzf $native
sudo tar -C $hadoop_lib --strip-components=2 -xvzf $jblas
sudo chown --recursive hadoop $hadoop_lib

# Add proper configs to hadoop-env.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:$hadoop_lib:\$LD_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-user-env.sh
echo "export JAVA_LIBRARY_PATH=$hadoop_lib:\$JAVA_LIBRARY_PATH" >> /home/hadoop/conf/hadoop-user-env.sh

# Add to bashrc, for good measure.
echo "export LD_LIBRARY_PATH=/usr/local/fwtools/usr/lib:$hadoop_lib:\$LD_LIBRARY_PATH" >> /home/hadoop/.bashrc
echo "export JAVA_LIBRARY_PATH=$hadoop_lib:\$JAVA_LIBRARY_PATH" >> /home/hadoop/.bashrc

exit 0
