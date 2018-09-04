test.sh#!/usr/bin/env bash

set -e

if [ "$MODULES" == '' ]; then
    echo "Running integration test script: mvn -q verify -P travis,integration-test -B"
    mvn -q verify -P travis,integration-test -B
else
    echo "Running integration test script: mvn -q verify -P travis,integration-test -B -pl $MODULES"
    mvn -q verify -P travis,integration-test -B -pl $MODULES
fi
