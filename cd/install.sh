#!/usr/bin/env bash

if [ "$MODULES" == '' ] || [[ $MODULES == *'!'* ]]; then
    echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V"
    mvn -T 2C -q install -P quick,travis,build-extras -B -V
else
    echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V -pl $MODULES -am"
    mvn -T 2C -q install -P quick,travis,build-extras -B -V -pl $MODULES -am
fi
