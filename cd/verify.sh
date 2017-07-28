#!/usr/bin/env bash

if [ "$MODULES" == '' ]; then
    echo "Running verify script: mvn -q verify -P travis -B"
    mvn -q verify -P travis -B
else
    echo "Running verify script: mvn -q verify -P travis -B -pl $MODULES"
    mvn -q verify -P travis -B -pl $MODULES
fi
