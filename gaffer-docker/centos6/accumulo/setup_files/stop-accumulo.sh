#!/bin/bash
. /home/hduser/.bashrc
cd /opt/accumulo/logs && /opt/accumulo/bin/stop-all.sh
/opt/hadoop/bin/hdfs dfs -rm -r /accumulo
/home/hduser/start-zookeeper.sh stop
/home/hduser/stop-hadoop.sh
