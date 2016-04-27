#!/bin/bash
. /home/hduser/.bashrc

/opt/hadoop/bin/hdfs namenode -format

echo -e "\n"

/opt/hadoop/sbin/start-dfs.sh

echo -e "\n"
/opt/hadoop/sbin/start-yarn.sh
