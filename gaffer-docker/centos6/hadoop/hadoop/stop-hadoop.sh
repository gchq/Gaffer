#!/bin/bash
. /home/hduser/.bashrc
$HADOOP_INSTALL/sbin/stop-yarn.sh

echo -e "\n"
$HADOOP_INSTALL/sbin/stop-dfs.sh
