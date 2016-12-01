#!/usr/bin/env bash

#if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
#    mvn deploy -P sign,build-extras --settings cd/mvnsettings.xml
#fi

# Avoid check for testing purposes
mvn deploy -P sign,build-extras --settings cd/mvnsettings.xml