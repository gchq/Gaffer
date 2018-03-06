#!/usr/bin/env bash

if [ -z "$1" ]; then
    echo "Usage: ./updateKorypheVersion.sh <new version>"
    exit 1
fi

set -e

git reset --hard
git clean -fd
git checkout develop
git pull

newVersion=$1

git checkout -b updating-koryphe-version-$newVersion


mvn -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.parent.version
oldVersion=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=koryphe.version | grep -v '\['`


sed -i '' "s/<koryphe.version>$oldVersion</<koryphe.version>$newVersion</g" pom.xml
sed -i '' "s/uk.gov.gchq.koryphe:koryphe:$oldVersion/uk.gov.gchq.koryphe:koryphe:$newVersion/g" NOTICES

git add .
git commit -a -m "Updated Koryphe version to $newVersion"
git push -u origin updating-koryphe-version-$newVersion
