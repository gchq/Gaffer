#!/usr/bin/env bash

set -e

mkdir "/tmp/gaffer test/"
cp -R ./* "/tmp/gaffer test/"
cd "/tmp/gaffer test"
mvn package -Ptest
cd -
rm -rf "/tmp/gaffer test/"
