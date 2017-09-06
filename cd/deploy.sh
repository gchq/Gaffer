#!/usr/bin/env bash

set -e

if [ "$MODULES" == '' ]; then
    mvn -q deploy -P sign,build-extras,quick --settings cd/mvnsettings.xml -B
else
    mvn -q deploy -P sign,build-extras,quick --settings cd/mvnsettings.xml -B -pl $MODULES
fi