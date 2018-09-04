test.sh#!/usr/bin/env bash

set -e

if [ "$MODULES" == '' ]; then
    echo "Running unit test script: mvn -q verify -P travis,unit-test -B"
    mvn -q verify -P travis,unit-test -B
else
    echo "Running unit test script: mvn -q verify -P travis,unit-test -B -pl $MODULES"
    mvn -q verify -P travis,unit-test -B -pl $MODULES
fi
