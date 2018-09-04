test.sh#!/usr/bin/env bash

set -e

MODULES=$1
if [ "$MODULES" == '' ]; then
    echo "Running test script: mvn -q verify -P travis,test -B"
    mvn -q verify -P travis,test -B
else
    echo "Running test script: mvn -q verify -P travis,test -B -pl $MODULES"
    mvn -q verify -P travis,test -B -pl $MODULES
fi
