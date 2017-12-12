#!/usr/bin/env bash

# Run this script from the top level directory of this repository.
# Usage: ./example/road-traffic/scripts/start.sh [any extra mvn command arguments, e.g -am to build all dependencies]
mvn clean install -pl :road-traffic-demo -Proad-traffic-demo,quick $@
