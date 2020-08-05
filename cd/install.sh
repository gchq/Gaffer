#!/usr/bin/env bash

set -e

if [ "$RELEASE" != 'true' ]; then
    if [ "$TRAVIS_BRANCH" == 'develop' ] || [ "$TRAVIS_PULL_REQUEST" != 'false' ]; then
        ./cd/install_koryphe.sh
        if [ "$MODULES" == '' ] || [[ $MODULES == *'!'* ]]; then
            echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V"
            mvn install -P quick,travis,build-extras -V
        else
            echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V -pl $MODULES -am"
            mvn install -P quick,travis,build-extras -V -pl $MODULES -am
        fi
    fi
fi
