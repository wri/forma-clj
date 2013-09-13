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

# make sure apt repos are up to date
sudo apt-get update

# Start with screen.
sudo apt-get -y --force-yes install screen
sudo apt-get -y --force-yes install exim4
sudo apt-get -y --force-yes install tmux

# Install htop - helpful for monitoring slave nodes
sudo apt-get -y --force-yes install htop

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

# Convenient 'repl' command
echo "alias repl='screen -Lm hadoop jar /home/hadoop/forma-clj/target/forma-0.2.0-SNAPSHOT-standalone.jar clojure.main'" >> /home/hadoop/.bashrc

# Setup for git
sudo apt-get -y --force-yes install git
git config --global user.name "Whizbang Systems"
git config --global user.email "admin@whizbangsystems.net"

# generate ssh key
ssh-keygen -t rsa -N "" -f /home/hadoop/.ssh/id_rsa -C "admin@whizbangsystems.net"
sudo chmod 644 /home/hadoop/.ssh/id_rsa

# Add github to known_hosts
echo "github.com,207.97.227.239 ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" >> /home/hadoop/.ssh/known_hosts

# simple uberjarring
echo "alias uj='lein do deps, compile :all, uberjar'" >> /home/hadoop/.bashrc

# simple re-attach to tmux
echo "alias ta='tmux attach'" >> /home/hadoop/.bashrc

# add git autocomplete per
# http://mbuttu.wordpress.com/2011/07/11/git-autocomplete-for-bash-shells/
curl https://raw.github.com/git/git/master/contrib/completion/git-completion.bash > ~/.git-completion.bash
echo "source ~/.git-completion.bash" >> .bashrc

# simple leiningen install via 'li'
echo "alias li='cd /home/hadoop/bin; wget https://raw.github.com/technomancy/leiningen/preview/bin/lein; chmod u+x lein; ./lein; cd /home/hadoop;'" >> /home/hadoop/.bashrc

# clone forma-clj repo
echo "alias gc='git clone git://github.com/reddmetrics/forma-clj.git'" >> /home/hadoop/.bashrc

# alias to do standard forma-clj setup
setup="li; gc; cd forma-clj; uj; cd ~/;"

exit 0
