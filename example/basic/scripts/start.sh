#!/usr/bin/env bash

# Run this script from the top level directory of this repository.
# Usage: ./example/basic/scripts/start.sh [any extra mvn command arguments, e.g -am to build all dependencies]
mvn clean install -pl :basic-rest -Pbasic-demo,quick $@
