#! /bin/bash

echo "Starting new cluster for tests"
lein emr -s 1 -t large -b .25 -bs bsaconfig.xml

status=$(elastic-mapreduce --list --active | awk '/j/ {print $2}')

sleep 10

while [ "$status" = "STARTING" ] || [ "$status" = "BOOTSTRAPPING" ]
do
    if [ "$status" = "STARTING" ]
    then
        echo "Cluster is starting"
    else
        echo "Cluster is bootstrapping"
    fi
    sleep 30
    status=$(elastic-mapreduce --list --active | awk '/j/ {print $2}')
done

echo "Cluster is running"
echo
echo "Logging in to setup tests. Give this a few minutes to work."
echo "DNS: $(elastic-mapreduce --list --active | awk '/j/ {print $3}')" 
echo

# set up ssh commands
dns=$(elastic-mapreduce --list --active | awk '/j/ {print $3}')
id_file=~/.ssh/id_rsa-forma-keypair
ssh_cmd="ssh -i $id_file -o StrictHostKeyChecking=no hadoop@$dns"

# send commands to set up testing environment
$ssh_cmd 'git clone git@github.com:reddmetrics/forma-clj.git;
cd forma-clj;
git checkout develop;
cd ../bin;
wget https://raw.github.com/technomancy/leiningen/preview/bin/lein;
chmod u+x lein;
./lein;
cd ..;
tmux new-session -d;'

echo "Running 'lein do deps, compile :all, uberjar', then running tests."
echo

# send compile and test runner commands

$ssh_cmd 'tmux send-keys "cd; cd forma-clj; lein do deps, compile :all, uberjar; lein midje; cd ..;"'
$ssh_cmd tmux send-keys C-m

# print helpful info for easy login after script completes
echo "Login to $dns and run 'tmux attach' to check progress, 'C-b d' to detach."
echo "ssh -i ~/.ssh/id_rsa-forma-keypair hadoop@$dns"
 
