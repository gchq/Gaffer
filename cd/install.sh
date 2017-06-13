#!/usr/bin/env bash

if [ "$MODULES" == '' ] || [[ $MODULES == *'!'* ]]; then
    mvn -q install -P quick,travis,build-extras -B -V
else
    mvn -q install -P quick,travis,build-extras -B -V -pl $MODULES -am
fi
