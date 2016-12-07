def labels = ['Centos7', 'Debian8', 'WinServer2012R2']
def builders = [:]

for (x in labels) {

    def label = x

    builders[label] = {
        node() {
            def mvnHome
            stage('prepare') {
                jdk7 = tool name: 'java-7-openjdk'
                jdk8 = tool name: 'java-8-jdk'
                git url: "https://github.com/gchq/Gaffer.git", branch: "develop"
                mvnHome = tool 'M3'
            }
            stage('java-7-jdk') {
                env.JAVA_HOME = "${jdk7}"
                echo "jdk installation path is: ${jdk7}"
                sh "${jdk7}/bin/java -version"
                sh "'${mvnHome}/bin/mvn' clean"
                sh "'${mvnHome}/bin/mvn' -Dmaven.test.failure.ignore package"
            }
            stage('java-8-jdk') {
                env.JAVA_HOME = "${jdk8}"
                echo "jdk installation path is: ${jdk8}"
                sh "${jdk8}/bin/java -version"
                sh "'${mvnHome}/bin/mvn' clean"
                sh "'${mvnHome}/bin/mvn' -Dmaven.test.failure.ignore package"
            }
        }
    }
}

parallel builders