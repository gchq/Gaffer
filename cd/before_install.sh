#!/usr/bin/env bash

set -e

echo "All modules script: mvn -q -Dexec.executable='echo' -Dexec.args='\${project.artifactId}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec"
mvn -Dexec.executable='echo' -Dexec.args='\${project.artifactId}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec
allModules=`mvn -q -Dexec.executable='echo' -Dexec.args='\${project.artifactId}' org.codehaus.mojo:exec-maven-plugin:1.3.1:exec`
echo "All modules: $allModules"
for module in $allModules
do
    if ! grep -q :$module .travis.yml
    then
        echo ".travis.yml is missing module $module";
#        exit 1;
    fi
done
