#!/usr/bin/env bash

set -e

echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V"
rm -rf $HOME/.m2/repository/uk/gov/gchq/gaffer
mvn -q install -P quick,travis,build-extras -B -V
