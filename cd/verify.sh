#!/usr/bin/env bash

set -e

if [ "$RELEASE" != 'true' ] && [ "$TRAVIS_PULL_REQUEST" != 'false' ]; then
    if [ "$MODULES" == '' ]; then
        echo "Running verify script: mvn -q verify -P travis,analyze -B"
        mvn verify -P travis,analyze -B
        echo "Running verify script: mvn -q verify -P travis,test -B"
        mvn verify -P travis,test -B
    else
        echo "Running verify script: mvn -q verify -P travis,analyze -B -pl $MODULES"
        mvn verify -P travis,analyze -B -pl $MODULES
        echo "Running verify script: mvn -q verify -P travis,test -B -pl $MODULES"
        mvn verify -P travis,test -B -pl $MODULES
    fi
fi
