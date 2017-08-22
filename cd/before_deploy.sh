#!/usr/bin/env bash

openssl aes-256-cbc -K $encrypted_de949738249f_key -iv $encrypted_de949738249f_iv -in cd/codesigning.asc.enc -out cd/codesigning.asc -d
gpg --fast-import cd/codesigning.asc
