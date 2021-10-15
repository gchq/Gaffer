#!/usr/bin/env bash

if [ -z "$1" ]; then
    echo "Usage: ./updateKorypheVersion.sh <new version>"
    exit 1
fi

function is_version_incremented() {

    local _old_version="${1}"
    local _new_version="${2}"

    # No difference
    [[ "x${_old_version}" == "x${_new_version}" ]] && return 1

    local IFS=.
    local i _old_version_array=(${_old_version}) _new_version_array=(${_new_version})

    # Ensure both arrays have the same length by padding old version with zeros upto the length of the new version
    for ((i=${#_old_version_array[@]}; i<${#_new_version_array[@]}; i++))
    do
        _old_version_array[i]=0
    done

    for ((i=0; i<${#_old_version_array[@]}; i++))
    do
        # Populate empty fields in _new_version_array with zero
        [[ -z ${_new_version_array[i]} ]] && {
            _new_version_array[i]=0
        }
        # Old version is bigger
        [[ ((10#${_old_version_array[i]} > 10#${_new_version_array[i]})) ]] && {
            return 1
        }
        # New version is bigger
        [[ ((10#${_old_version_array[i]} < 10#${_new_version_array[i]})) ]] && {
            return 0
        }
    done
}

set -e

git reset --hard
git clean -fd
git checkout develop
git pull

newVersion=$1

mvn -q org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.parent.version
oldVersion=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=koryphe.version | grep -v '\['`

$(is_version_incremented "${oldVersion}" "${newVersion}") && {

    versionUpdateBranch="${VERSION_UPDATE_BRANCH:-updating-koryphe-version-$newVersion}"

    git checkout -b ${versionUpdateBranch}

    sed -i'' "s/<koryphe.version>$oldVersion</<koryphe.version>$newVersion</g" pom.xml
    sed -i'' "s/uk.gov.gchq.koryphe:koryphe:$oldVersion/uk.gov.gchq.koryphe:koryphe:$newVersion/g" NOTICES

    git add .
    git commit -a -m "Updated Koryphe version to $newVersion"
    git push -u origin ${versionUpdateBranch} || {
        echo "Unable to push branch ${versionUpdateBranch}, exiting."
        exit 1
    }

} || {

    echo "Unable to update Koryphe version: New version: ${newVersion} is not larger than existing version: ${oldVersion}."
    exit 1
}
