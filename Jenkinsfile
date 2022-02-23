pipeline {
    agent {
        kubernetes {
            yamlFile 'jenkins-pod.yaml'
        }
    }
    environment {
        // creates DOCKERHUB_USR and DOCKERHUB_PSW env variables
        DOCKERHUB = credentials('fondahub-dockerhub')
    }

    stages {
        stage('Build') {
            steps {
                container('maven') {
                    // run a clean build without tests to see if the project compiles
                    sh 'mvn clean test-compile -DskipTests=true -Dmaven.javadoc.skip=true -B -V'
                }
            }
        }

        stage('Test') {
            steps {
                container('maven') {
                    // run JUnit tests
                    sh 'mvn test -B -V'
                }
            }
            post {
                // collect test results
                always {
                    junit 'target/surefire-reports/TEST-*.xml'
                    jacoco classPattern: 'target/classes,target/test-classes', execPattern: 'target/coverage-reports/*.exec', inclusionPattern: '**/*.class', sourcePattern: 'src/main/java,src/test/java'
                    archiveArtifacts 'target/surefire-reports/TEST-*.xml'
                    archiveArtifacts 'target/coverage-reports/*.exec'
                }
            }
        }

        stage('Package') {
            steps {
                container('maven') {
                    sh 'mvn package -DskipTests=true -Dmaven.javadoc.skip=true -B -V'
                }
            }
            post {
                success {
                    archiveArtifacts 'target/*.jar'
                }
            }
        }

        stage('Static Code Analysis') {
            steps {
                container('maven') {
                    withSonarQubeEnv('fonda-sonarqube') {
                        sh """
                            println ${env.SONAR_HOST_URL}
                            println ${SONAR_AUTH_TOKEN}
                        """
                        sh '''
                            mvn sonar:sonar -B -V -Dsonar.projectKey=workflow_k8s_scheduler \
                                -Dsonar.branch.name=$BRANCH_NAME -Dsonar.sources=src/main/java -Dsonar.tests=src/test/java \
                                -Dsonar.inclusions="**/*.java" -Dsonar.test.inclusions="src/test/java/**/*.java" \
                                -Dsonar.junit.reportPaths=target/surefire-reports \
                                -Dsonar.jacoco.reportPaths=$(find target/coverage-reports -name '*.exec' | paste -s -d , -)
                        '''
                    }
                }
            }
        }

        stage('Build and push Docker') {
            // agents are specified per stage to enable real parallel execution
            parallel {
                stage('workflow-k8s-scheduler') {
                    agent {
                        kubernetes {
                            yamlFile 'jenkins-pod.yaml'
                        }
                    }
                    steps {
                        container('hadolint') {
                            sh "hadolint --format json Dockerfile | tee -a hadolint_scheduler.json"
                        }
                        // build and push image to fondahub/workflow-k8s-scheduler
                        container('docker') {
                            sh "echo $DOCKERHUB_PSW | docker login -u $DOCKERHUB_USR --password-stdin"
                            sh "docker build . -t fondahub/workflow-k8s-scheduler:${GIT_COMMIT[0..7]}"
                            sh "docker tag fondahub/workflow-k8s-scheduler:${GIT_COMMIT[0..7]} fondahub/workflow-k8s-scheduler:latest"
                            sh "docker push fondahub/workflow-k8s-scheduler:${GIT_COMMIT[0..7]}"
                            sh "docker push fondahub/workflow-k8s-scheduler:latest"
                        }
                    }
                    post {
                        always {
                            archiveArtifacts "hadolint_scheduler.json"
                            recordIssues(
                                aggregatingResults: true,
                                tools: [hadoLint(pattern: "hadolint_scheduler.json")]
                            )
                        }
                    }
                }
                stage('vsftpd') {
                    agent {
                        kubernetes {
                            yamlFile 'jenkins-pod.yaml'
                        }
                    }
                    steps {
                        container('hadolint') {
                            sh "hadolint --format json daemons/ftp/Dockerfile | tee -a hadolint_vsftpd.json"
                        }
                        // build and push image to fondahub/vsftpd
                        container('docker') {
                            sh "echo $DOCKERHUB_PSW | docker login -u $DOCKERHUB_USR --password-stdin"
                            sh "docker build daemons/ftp/ -t fondahub/vsftpd:${GIT_COMMIT[0..7]}"
                            sh "docker tag fondahub/vsftpd:${GIT_COMMIT[0..7]} fondahub/vsftpd:latest"
                            sh "docker push fondahub/vsftpd:${GIT_COMMIT[0..7]}"    
                            sh "docker push fondahub/vsftpd:latest"
                        }
                    }
                    post {
                        always {
                            archiveArtifacts "hadolint_vsftpd.json"
                            recordIssues(
                                aggregatingResults: true,
                                tools: [hadoLint(pattern: "hadolint_vsftpd.json")]
                            )
                        }
                    }
                }
            }
        }
    }
}