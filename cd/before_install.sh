#!/usr/bin/env bash

set -e

# Need to list all the modules. This needs to be done without using maven
# as we don't want to download dependencies and compile the code first.
# This method isn't very nice but we needed a simple approach that didn't
# require external dependencies that weren't available on travis.
allModules=""
pomPaths=`find . -name "pom.xml"`

for pomPath in $pomPaths
do
    currentModule=`cat $pomPath | grep '^    <artifactId>*' | sort -r | head -1 | cut -d '>' -f 2 | cut -d '<' -f 1`
    allModules="$allModules $currentModule"
done

for module in $allModules
do
    if ! grep -q :$module .travis.yml
    then
        echo ".travis.yml is missing module $module";
        exit 1;
    fi
done
