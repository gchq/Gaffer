#!/usr/bin/env bash

if [ "$MODULES" == '' ]; then
    if [ "$PROFILE" == '' ]; then
        mvn -q verify -P travis -B
    else
        mvn -q verify -P travis -B -P $PROFILE
    fi
else
    if [ "$PROFILE" == '' ]; then
        mvn -q verify -P travis -B -pl $MODULES
    else
        mvn -q verify -P travis -B -P $PROFILE -pl $MODULES
    fi
fi
