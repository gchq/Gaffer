#!/usr/bin/env groovy

def labels = ['Centos7', 'Debian8']
def envs = ['java-8-jdk']
def builders = [:]

for (x in labels) {
    def label = x
    for (y in envs) {
        def env = y

        builders[label + ":" + env] = {
            node(label) {
                def mvnHome = tool name: 'M3'
                def jdk = tool name: env
                stage('checkout') {
                    checkout scm
                }
                stage('test') {
                    echo "jdk installation path is: ${jdk}"
                    sh "${jdk}/bin/java -version"
                    sh "'${mvnHome}/bin/mvn' clean"
                    sh "'${mvnHome}/bin/mvn' -Dmaven.test.failure.ignore install"
                    sh "'${mvnHome}/bin/mvn' test integration-test -Pserialisation-compatibility -Dmaven.test.failure.ignore -pl :serialisation"
                }
            }
        }
    }
}

parallel builders