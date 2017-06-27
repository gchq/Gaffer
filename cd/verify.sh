#!/usr/bin/env bash

if [ "$MODULES" == '' ]; then
    if [ "$PROFILE" == '' ]; then
        echo "Running verify script: mvn -q verify -P travis -B"
        mvn -q verify -P travis -B
    elif [ "$PROFILE" == 'analyze' ]; then
        # Don't run in quiet mode
        echo "Running verify script: mvn verify -P travis -B -P analyze"
        mvn verify -P travis -B -P analyze
    else
        echo "Running verify script: mvn -q verify -P travis -B -P $PROFILE"
        mvn -q verify -P travis -B -P $PROFILE
    fi
else
    if [ "$PROFILE" == '' ]; then
        echo "Running verify script: mvn -q verify -P travis -B -pl $MODULES"
        mvn -q verify -P travis -B -pl $MODULES
    else
        echo "Running verify script: mvn -q verify -P travis -B -P $PROFILE -pl $MODULES"
        mvn -q verify -P travis -B -P $PROFILE -pl $MODULES
    fi
fi
