#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    mvn -q deploy -P sign,build-extras,quick --settings cd/mvnsettings.xml -B
fi
