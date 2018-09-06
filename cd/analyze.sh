#!/usr/bin/env bash

set -e

echo "Running analyze script: mvn -q verify -P travis,analyze -B"
mvn -q verify -P travis,analyze -B
