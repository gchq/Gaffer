#!/bin/bash

/home/hduser/start-hadoop.sh
/home/hduser/start-zookeeper.sh
sleep 10
#/opt/accumulo/bin/accumulo init -u root --instance-name Test --password admin
/opt/accumulo/bin/accumulo init --instance-name Gaffer --password admin
sleep 10
# Start accumulo

/opt/accumulo/bin/start-all.sh
