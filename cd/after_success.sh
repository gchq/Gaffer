#!/usr/bin/env bash

set -e

if [ "$RELEASE" = 'true' ] && [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    ./cd/before_deploy.sh
    ./cd/deploy.sh
else
    bash <(curl -s https://codecov.io/bash)
fi
