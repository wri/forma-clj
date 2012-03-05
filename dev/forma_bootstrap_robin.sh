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

# a few things for convenience
echo "alias repl='screen -Lm hadoop jar /home/hadoom/forma-clj/forma-0.2.0-SNAPSHOT-standalone.jar clojure.main'" >> /home/hadoop/.bashrc

# setup for git
sudo apt-get -y --force-yes install git
mkdir .ssh

# ssh-keygen -t rsa -N "" -f "/hadoop/hadoop/.ssh/instance" -C "rkraft4@gmail.com"

echo '-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAvBPP+vTyCxnC0jZt+ASt+3fD/0LenU2zrBe5F8GVmhb964xV
1UojASmtuZqvk8RqaS6PXRi2VGxwkVVDjS8l9kciF5a2l4D0b4Z+dpefciyzoE1T
2rJkeHYTP2p9O6EncQmHvZxTKbNQ7SQh6yxoFHKATglC9OlJfhYntTjqywZvYvgl
26ty69E9bqcltm1yIil8XdSn+vm+IbDxTVeAdCFMYFKDdegvG63B5sL8nJANN9sW
ALG2/e16LO7Kmha1wFD2rLljwia/Zl4mNdf5CIF/WlgCD5OnzsqzADnn+1tjlj4A
NNthxaW+z3tK3/pQO3ANl2Tn7NMWsk20RevdJwIDAQABAoIBAF7uBSEfN3hg5VPj
QzhXbFWsCtKxttlhGdo4EyWpgVBIYJvetog0plx05An1yL9l+Wvjo1sTGRydq2e/
yJvfe2LGXq/XU4w++6G7GePT/hfL9lJoFXYiatHejzKIFnPdkKHedJRA7jzzFFrN
zQz92f7QGHDK/e/OPFkW563x75jPZioWCSlvDHVUP8U/ChTSs/geMnSdedfmFZpX
5rpG0nG1/td4M1DEsTEIt/4oTMmk3ZK/d9FK8NVZT/0mIavsGExDhH4dL1Mw66JY
sejtlxh6w/Jz4aFWuJ1dIiqsyhgf4rQZxuydn4VWt6LOWRNWnAaqQEGqh9xPoPYK
1KC7yuECgYEA3uz7ZM0bOVZfnlDfD0S4ClBeKorAlwGw50Fkq+pPyOM7zwBozAzN
6pmv7ebaqnFhfGIH6F8+s/1EFBOaRupkOXkR05xa8mBvxgRiTACPyTczAop5bsZV
ZVABgRSnQGMGTUqQJEp2Dr8HSInFAemoYYF1Y+WgYaFSV1AxD6Mr7jECgYEA1/s+
lJ+Vcxrt4QKwS5jWwzAnuCzGRRPwOS6XCZX42EJlV6CyOqhGwnxzeB1oD+hJykEW
evoAV10ce82sGT42rqQpv4T3yevW4jPFedA+rOFN5WVTyFfj9/ex7WR1CAgxV31O
Uf623KRUy1ALNtj15Z2RJDwym8WjBZBA8xUFctcCgYB8MktcvWiNaTGcYjHjr8VY
+a18xhDGEIseS7BqlwAcS3zmtrOr+vY93aHGSPdKPiCxy2vcajPk0xU6mjE5kRyF
aI4l9tY0csXS7F5XOXjiAX7Jy5wbuyOYbhpob7k+hezc4s2ralCbppHIN/kqN6M3
r4hXP3c67UWSn3q125J+AQKBgFJ5jKIwdl9oDyJ3Zl7X1FrgzqoT3vqN7JPJaL+u
V0ItyIk3wheIHs0xvN5HTG+Wombrh+wZ/3tdAP19wpQ5H6R857xMyFqBBqOGZYho
ryZROu+4S9AbT+Bm47jlPZN9nWntXbUN8UI8Nm0U+dNN8khmbQBLRIAbbm83cLXi
dxwtAoGBAJSx5Kf4wSzjmnVz8q4MA3LwCpuF54ZAbf+hhhOioEdHKUp+NRL9CjpP
Bx7mDb0xI/K9gjO1y2aY3zE2hH14cWulXwGKKJwH0VAdqwF4+WW9rq5IKr5XywH0
4zTeZUepNBKu7LYjFjJdFg/9qbfy0Qc5o6L5Nt/8dMQm165FOmz3
-----END RSA PRIVATE KEY-----' > /home/hadoop/.ssh/id_rsa
echo 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC8E8/69PILGcLSNm34BK37d8P/Qt6dTbOsF7kXwZWaFv3rjFXVSiMBKa25mq+TxGppLo9dGLZUbHCRVUONLyX2RyIXlraXgPRvhn52l59yLLOgTVPasmR4dhM/an07oSdxCYe9nFMps1DtJCHrLGgUcoBOCUL06Ul+Fie1OOrLBm9i+CXbq3Lr0T1upyW2bXIiKXxd1Kf6+b4hsPFNV4B0IUxgUoN16C8brcHmwvyckA032xYAsbb97Xos7sqaFrXAUPasuWPCJr9mXiY11/kIgX9aWAIPk6fOyrMAOef7W2OWPgA022HFpb7Pe0rf+lA7cA2XZOfs0xayTbRF690n rkraft4@gmail.com' > /home/hadoop/.ssh/id_rsa.pub
chmod 600 /home/hadoop/.ssh/id_rsa

echo "github.com,207.97.227.239 ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAq2A7hRGmdnm9tUDbO9IDSwBK6TbQa+PXYPCPy6rbTrTtw7PHkccKrpp0yVhp5HdEIcKr6pLlVDBfOLX9QUsyCOV0wzfjIJNlGEYsdlLJizHhbn2mUjvSAHQqZETYP81eFzLQNnPHt4EVVUh7VfDESU84KezmD5QlWpXLmvU31/yMf+Se8xhHTvKSCZIFImWwoG6mbUoWf9nzpIoaSjB+weqqUUmpaaasXVal72J+UX2B+2RPW3RcT0eOzQgqlJL3RKrTJvdsjE3JEAvGq3lGHSZXy28G3skua2SmVi/w4yCE6gbODqnTWlg7+wC604ydGXA8VJiS5ap43JXiUFFAaQ==" >> /home/hadoop/.ssh/known_hosts
cd /home/hadoop/

source /home/hadoop/.bashrc

# get lein and install - easier to edit/compile on cluster
cd bin
wget https://raw.github.com/technomancy/leiningen/stable/bin/lein
chmod u+x lein
cd ..

git clone git@github.com:sritchie/forma-clj.git
cd forma-clj

# run once
lein
lein deps
lein uberjar

exit 0
