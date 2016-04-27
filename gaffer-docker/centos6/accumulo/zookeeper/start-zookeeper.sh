#!/bin/bash
. /home/hduser/.bashrc
if [ "$1" == "stop" ]; then
cd /opt/zookeeper/data
/opt/zookeeper/bin/zkServer.sh stop
else
cd /opt/zookeeper/data
/opt/zookeeper/bin/zkServer.sh start
fi
