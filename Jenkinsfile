pipeline {
    agent any

    environment {
        DOCKER_IMAGE = 'your-docker-image-name'
        KUBECONFIG_CREDENTIALS_ID = 'kubeconfig'  // Jenkins credential ID for kubeconfig
        REGISTRY_CREDENTIALS_ID = 'docker-registry'  // Jenkins credential ID for Docker registry
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Docker Image') {
            steps {
                script {
                    docker.build("${DOCKER_IMAGE}:${env.BUILD_ID}")
                }
            }
        }

        stage('Push Docker Image') {
            steps {
                script {
                    docker.withRegistry('https://your-registry-url', "${REGISTRY_CREDENTIALS_ID}") {
                        docker.image("${DOCKER_IMAGE}:${env.BUILD_ID}").push()
                    }
                }
            }
        }

        stage('Deploy to Kubernetes') {
            steps {
                script {
                    withCredentials([file(credentialsId: "${KUBECONFIG_CREDENTIALS_ID}", variable: 'KUBECONFIG')]) {
                        sh '''
                        kubectl apply -f k8s/deployment.yaml
                        kubectl apply -f k8s/service.yaml
                        '''
                    }
                }
            }
        }
    }
}
