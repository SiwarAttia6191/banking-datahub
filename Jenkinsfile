// same stages as .gitlab-ci.yml, for the environments that run jenkins
// prerequisites on the agent: jdk 17 and sbt on the PATH (or configured via
// the jenkins global tool configuration)

pipeline {
    agent any

    options {
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }

    stages {
        stage('Compile') {
            steps {
                sh 'sbt compile'
            }
        }
        stage('Test') {
            steps {
                sh 'sbt test'
            }
        }
        stage('Package') {
            steps {
                sh 'sbt "ingestion/assembly" "transform/assembly" "quality/assembly" "baseline/assembly"'
                archiveArtifacts artifacts: 'modules/*/target/scala-2.12/*assembly*.jar',
                                 fingerprint: true
            }
        }
    }

    post {
        success {
            echo 'pipeline green'
        }
        failure {
            echo 'pipeline failed - check the stage logs'
        }
    }
}
