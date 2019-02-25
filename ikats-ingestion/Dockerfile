FROM maven:3.5.2-jdk-8 as war-build

LABEL license="Apache License, Version 2.0"
LABEL copyright="CS Systèmes d'Information"
LABEL maintainer="contact@ikats.org"
LABEL version="0.9.1"

WORKDIR /usr/src/app
# /root/.m2 is a volume :(
# Build cache fix provided by https://github.com/carlossg/docker-maven/issues/36#issuecomment-334562850
ENV MAVEN_OPTS="-Dmaven.repo.local=/usr/src/app/m2repo/"

## Step 1 -- Build the TDM project artifacts and install them into the maven local repository

# Add only the maven pom.xml
# this will help us to cache the maven repository into docker cache
COPY ikats-datamodel/pom.xml ikats-datamodel/pom.xml
COPY ikats-commons/pom.xml ikats-commons/pom.xml
COPY dbWebclient/pom.xml dbWebclient/pom.xml
COPY TemporalDataManagerWebApp/pom.xml TemporalDataManagerWebApp/pom.xml
COPY pom.xml pom.xml

# get all the downloads out of the way
RUN mvn -B -e -C -T 1C org.apache.maven.plugins:maven-dependency-plugin:3.0.2:go-offline --fail-never

# Now add the other sources and package the whole into the WAR
COPY ikats-datamodel ikats-datamodel
COPY ikats-commons ikats-commons
COPY dbWebclient dbWebclient
COPY TemporalDataManagerWebApp TemporalDataManagerWebApp
RUN mvn clean install -DskipTests=true

## Step 2 -- Build the IKATS Ingestion app
COPY ikats-ingestion/pom.xml ikats-ingestion/pom.xml
WORKDIR /usr/src/app/ikats-ingestion
RUN mvn -B -e -C -T 1C org.apache.maven.plugins:maven-dependency-plugin:3.0.2:go-offline --fail-never

COPY LICENSE .
COPY NOTICE .
COPY ikats-ingestion/src src
RUN mvn -B -e -o -T 1C clean package -DskipTests=true

#
# Creates the Docker image for installing the IKATS Ingestion Service
#
# Dockerfile original source from https://github.com/docker-library/docs/tree/master/tomee
# Flavor : 8-jdk-7.0.1-plume (8-jdk-7.0.1-plume/Dockerfile)

FROM openjdk:8

LABEL license="Apache License, Version 2.0"
LABEL copyright="CS Systèmes d'Information"
LABEL maintainer="contact@ikats.org"
LABEL version="0.9.1"

# install zip in order to repack war after configuration injection
RUN apt-get update && apt-get install -y --no-install-recommends zip \
	&& rm -rf /var/lib/apt/lists/*

ENV PATH /usr/local/tomee/bin:$PATH
RUN mkdir -p /usr/local/tomee

WORKDIR /usr/local/tomee

RUN set -x \
	&& curl -fsSL 'https://www.apache.org/dist/tomee/KEYS' -o KEYS \
	&& gpg --no-tty --import KEYS

RUN set -x \
	&& curl -fSL http://repo.maven.apache.org/maven2/org/apache/tomee/apache-tomee/7.0.4/apache-tomee-7.0.4-plume.tar.gz.asc -o tomee.tar.gz.asc \
	&& curl -fSL http://repo.maven.apache.org/maven2/org/apache/tomee/apache-tomee/7.0.4/apache-tomee-7.0.4-plume.tar.gz -o tomee.tar.gz \
	&& gpg --batch --verify tomee.tar.gz.asc tomee.tar.gz \
	&& tar -zxf tomee.tar.gz \
	&& mv apache-tomee-plume-7.0.4/* /usr/local/tomee \
	&& rm -Rf apache-tomee-plume-7.0.4 \
	&& rm bin/*.bat \
	&& rm tomee.tar.gz*

# Copy the modified configuration for IKATS
COPY ikats-ingestion/assets/tomee-conf/. .

# Reclaim the war package at the previous multi-stage
COPY --from=war-build /usr/src/app/ikats-ingestion/target/ikats-ingestion.war webapps/.

WORKDIR /tmp/container_init
COPY ikats-ingestion/assets/container_init.sh .
COPY ikats-ingestion/assets/inject_configuration.sh .

# Declare a shared volume
VOLUME ["/mnt/IKATSDATA"]

# Expose functional and debug ports
EXPOSE 8081 9010 9011

# Run a script to replace target dependent values into the templated war configuration
# and launch the Tomcat
ENTRYPOINT ["sh", "container_init.sh"]

