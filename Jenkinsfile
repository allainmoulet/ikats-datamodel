pipeline {
    agent any
    tools { 
        maven 'Maven 3.5.2' 
        jdk 'JDK 1.8' 
    }
	parameters {
        string(name: 'DB_HOST', defaultValue: 'pgsql.dev', description: 'Hôte PostgreSQL')
        string(name: 'DB_PORT', defaultValue: '5432', description: 'Port PostgreSQL')
        string(name: 'TSDB_HOST', defaultValue: 'opentsdb_read', description: 'Hôte OpenTSDB')
        string(name: 'TSDB_PORT', defaultValue: '4242', description: 'Port OpenTSDB')
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
	    	environment {
			    DB_HOST = "${params.DB_HOST}"
				DB_PORT = "${params.DB_PORT}"
				TSDB_HOST = "${params.TSDB_HOST}"
				TSDB_PORT = "${params.TSDB_PORT}"
				C3P0_ACQUIRE_INCREMENT = "2"
				C3P0_MAX_SIZE = "20"
				C3P0_IDLE_TEST_PERIOD = "50"
				C3P0_MAX_STATEMENTS = "15"
				C3P0_MIN_SIZE = "5"
				C3P0_TIMEOUT = "90"
	    	}
	    	
	        steps {
		        script {
					datamodelImage = docker.build("ikats-datamodel",
													"--build-arg DB_HOST=${DB_HOST} "
													+ "--build-arg DB_PORT=${DB_PORT} "
													+ "--build-arg TSDB_HOST=${TSDB_HOST} "
													+ "--build-arg TSDB_PORT=${TSDB_PORT} "
													+ "--build-arg C3P0_ACQUIRE_INCREMENT=${C3P0_ACQUIRE_INCREMENT} "
													+ "--build-arg C3P0_MAX_SIZE=${C3P0_MAX_SIZE} "
													+ "--build-arg C3P0_IDLE_TEST_PERIOD=${C3P0_IDLE_TEST_PERIOD} "
													+ "--build-arg C3P0_MAX_STATEMENTS=${C3P0_MAX_STATEMENTS} "
													+ "--build-arg C3P0_MIN_SIZE=${C3P0_MIN_SIZE} "
													+ "--build-arg C3P0_TIMEOUT=${C3P0_TIMEOUT} "
													+ " .")
					
					fullBranchName = "${env.BRANCH_NAME}"
					branchName = fullBranchName.substring(fullBranchName.lastIndexOf("/") + 1)
					
		        	docker.withRegistry("${env.REGISTRY_ADDRESS}", 'DOCKER_REGISTRY') {
		    			/* Push the container to the custom Registry */
		    			datamodelImage.push(branchName + "_${GIT_COMMIT}")
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