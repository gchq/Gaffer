node {
    def mvnHome
    stage('prepare') {
        jdk7 = tool name: 'java-7-openjdk'
        jdk8 = tool name: 'java-8-jdk'
    }
    stage('checkout') {
        git url: "https://github.com/gchq/Gaffer.git", branch: "develop"
        mvnHome = tool 'M3'
    }
    stage('clean') {
        sh "'${mvnHome}/bin/mvn' clean"
    }
    stage('java-7-jdk') {
        env.JAVA_HOME = "${jdk7}"
        echo "jdk installation path is: ${jdk7}"
        sh "${jdk7}/bin/java -version"
        sh "'${mvnHome}/bin/mvn' -Dmaven.test.failure.ignore package"
    }
    stage('clean') {
        sh "'${mvnHome}/bin/mvn' clean"
    }
    stage('java-8-jdk') {
        env.JAVA_HOME = "${jdk8}"
        echo "jdk installation path is: ${jdk8}"
        sh "${jdk8}/bin/java -version"
        sh "'${mvnHome}/bin/mvn' -Dmaven.test.failure.ignore package"
    }
}