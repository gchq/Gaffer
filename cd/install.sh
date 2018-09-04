#!/usr/bin/env bash

set -e

echo "Running install script: mvn -q install -P quick,travis,build-extras -B -V"
mvn -q install -P quick,travis,build-extras -B -V
