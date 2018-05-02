pipeline {
    agent any
    tools {
        maven 'Maven 3.5.2'
        jdk 'JDK 1.8'
    }

    stages {
        stage('Fetch SCM') {
            steps {
                checkout scm
            }
        }

        stage('Unit Tests') {
            steps {
                withMaven(
                    maven: 'Maven 3.5.2',
                    mavenSettingsConfig: 'e924d227-1005-4fcb-92ef-3d382c066f09'
                ) {
                    sh 'mvn clean install -DskipTests'
                    sh 'mvn test'
                }
            }
            post {
                success {
                    junit '**/target/surefire-reports/**/*.xml'
                    step( [ $class: 'JacocoPublisher' ] )
                }
            }
        }
        stage('Build the image') {
            agent { node { label 'docker' } }

            steps {
                script {
                    datamodelImage = docker.build("ikats-datamodel", "--pull .")

                    fullBranchName = "${env.BRANCH_NAME}"
                    branchName = fullBranchName.substring(fullBranchName.lastIndexOf("/") + 1)
                    shortCommit = "${GIT_COMMIT}".substring(0, 7)

                    docker.withRegistry("${env.REGISTRY_ADDRESS}", 'DOCKER_REGISTRY') {
                        /* Push the container to the custom Registry */
                        datamodelImage.push(branchName + "_" + shortCommit)
                        datamodelImage.push(branchName + "_latest")
                          if (branchName == "master") {
                            datamodelImage.push("latest")
                          }
                    }
                }
            }
        }
    }
}
