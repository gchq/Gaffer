#!/usr/bin/env bash

if [ "$RELEASE" != 'true' ]; then
    if [ "$MODULES" == '' ] || [[ $MODULES == *'!'* ]]; then
        echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V"
        mvn -q install -P quick,travis,build-extras -B -V
    else
        echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V -pl $MODULES -am"
        mvn -q install -P quick,travis,build-extras -B -V -pl $MODULES -am
    fi
fi
