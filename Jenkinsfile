def labels = ['Centos7', 'Debian8']
def envs = ['java-7-openjdk', 'java-8-jdk']
def builders = [:]

for (x in labels) {
    def label = x
    for (y in envs) {
        def env = y

        builders[label + ":" + env] = {
            node(label) {
                def mvnHome = tool name: 'M3'
                def jdk = tool name: env
                println jdk
                println env
                println mvnHome
                stage('prepare') {
                    git url: "https://github.com/gchq/Gaffer.git", branch: "develop"
                }
                stage('package') {
                    echo "jdk installation path is: ${jdk}"
                    sh "${jdk}/bin/java -version"
                    sh "'${mvnHome}/bin/mvn' clean"
                    sh "'${mvnHome}/bin/mvn' -Dmaven.test.failure.ignore package"
                }
            }
        }
    }
}

parallel builders
