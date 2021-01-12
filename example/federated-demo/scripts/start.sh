#!/usr/bin/env bash
# todo update all the scripts so they work
# Run this script from the top level directory of this repository.
# Usage: ./example/federated-demo/scripts/start.sh [any extra mvn command arguments, e.g -am to build all dependencies]
mvn clean install -pl :federated-demo -Pfederated-demo,quick $@
