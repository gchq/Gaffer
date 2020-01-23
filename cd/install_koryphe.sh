#!/usr/bin/env bash

set +e
# Some quick command to see if the pom is resolvable
koryphe_version=`mvn help:evaluate -Dexpression=koryphe.version -q -DforceStdout`
return_value=$?
set -e
if [[ ${return_value} -ne 0 ]]; then
    echo "Building Koryphe from source"
    cd .. && git clone https://github.com/gchq/koryphe.git && cd koryphe && git checkout master && mvn clean install -Pquick -q
fi