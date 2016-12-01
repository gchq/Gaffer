#!/usr/bin/env bash

#if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
#  openssl aes-256-cbc -K $encrypted_de949738249f_key -iv $encrypted_de949738249f_iv -in codesigning.asc.enc -out codesigning.asc -d
#  gpg --fast-import cd/codesigning.asc
#fi

# Avoid check for testing purposes
openssl aes-256-cbc -K $encrypted_de949738249f_key -iv $encrypted_de949738249f_iv -in codesigning.asc.enc -out codesigning.asc -d
gpg --fast-import cd/codesigning.asc