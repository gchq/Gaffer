test.sh#!/usr/bin/env bash

set -e

echo "Running test script: mvn -q verify -P travis,test -B"
mvn -q verify -P travis,integration-test -B
