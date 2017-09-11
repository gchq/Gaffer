#!/usr/bin/env bash

set -e

if [ "$RELEASE" = 'true' ]; then
    if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
        ./cd/before_deploy.sh
        ./cd/deploy.sh
    fi
else
    bash <(curl -s https://codecov.io/bash)
fi
